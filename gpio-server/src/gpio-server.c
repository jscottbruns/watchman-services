#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <syslog.h>

#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>

#include <assert.h>
#include <utils.h>

#include "ini.h"
#include "msw.h"
#include "zdb.h"
#include "log.h"
#include "gpio-server.h"

/* Globals */
ConnectionPool_T pool;
IniConfig ini_config;

volatile sig_atomic_t CONTINUE;
int DEBUG, pidFilehandle;

struct params {
	pthread_mutex_t SlotInUse;
	int ActiveSlot;
};

typedef struct params params_t;
params_t SlotLock;

void signal_handler(int sig)
{
	switch(sig)
    {
		case SIGHUP:
			LOG("Received SIGHUP (%d).", E_WARN, sig);
			break;

		case SIGINT:
		case SIGTERM:
			LOG("Received shutdown signal (%s). Closing gpio-server. ", E_WARN, strsignal(sig));
			CONTINUE = 1;
			break;

		default:
			LOG("Unhandled signal (%s)", E_WARN, strsignal(sig));
			break;
    }
}

void daemonize()
{
	pid_t pid, sid;
	char pidstr[10];
	int i, pretval;
	struct sigaction sigact;
	sigset_t newSigSet;

	syslog(LOG_INFO, "Initializing daemon environment");

	/* Check if parent process id is set */
	if (getppid() == 1)
	{
		/* PPID exists, therefore we are already a daemon */
		syslog(LOG_INFO, "Already running in daemon mode. Aborting daemon initialization process.");
		return;
	}

	/* Signal handlers */
	syslog(LOG_INFO, "Installing signal handlers");

	sigemptyset(&newSigSet);
	sigaddset(&newSigSet, SIGCHLD);  /* ignore child - i.e. we don't need to wait for it */
	sigaddset(&newSigSet, SIGTSTP);  /* ignore Tty stop signals */
	sigaddset(&newSigSet, SIGTTOU);  /* ignore Tty background writes */
	sigaddset(&newSigSet, SIGTTIN);  /* ignore Tty background reads */
	sigprocmask(SIG_BLOCK, &newSigSet, NULL);   /* Block the above specified signals */

	memset(&sigact, 0, sizeof(sigact));
	sigact.sa_handler = signal_handler;
	sigemptyset(&sigact.sa_mask);
	sigact.sa_flags = 0;

	sigaction(SIGHUP, &sigact, NULL); /* Catch hangup signals */
	sigaction(SIGINT, &sigact, NULL); /* Catch interupt signal */
	sigaction(SIGTERM, &sigact, NULL); /* Catch term signal */

	syslog(LOG_INFO, "Forking main worker process");

	/* Fork the new working process */
	pid = fork();
	if ( pid < 0 )
	{
		syslog(LOG_INFO, "Fork error (%d). Fatal.", pid);
		fprintf(stderr, "Fork error (%d). Fatal.", pid);
		exit(EXIT_FAILURE);
	}
	else if ( pid > 0 )
	{
		syslog(LOG_INFO, "Main worker process now running under PID %d. Parent exiting.", pid);
		exit(EXIT_SUCCESS);
	}

	/* Change the file mode mask */
	syslog(LOG_INFO, "Setting file mask mode (0644)");
	umask(0644);

	/* Set new process group */
	sid = setsid();

	syslog(LOG_INFO, "Setting process group (%d)", sid);

	if ( sid < 0 )
	{
		syslog(LOG_INFO, "Error setting process group (%d). Fatal.", sid);
		fprintf(stderr, "Error setting process group (%d). Fatal.", sid);
		exit(EXIT_FAILURE);
	}

	/* Route I/O connections */
	syslog(LOG_INFO, "Rerouting I/O connections to /dev/null.");

	/* Open stdin */
	i = open("/dev/null", O_RDWR);

	dup(i); /* STDOUT */
	dup(i); /* STDERR */

	chdir("/"); /* Change working directory */

	syslog(LOG_INFO, "Creating pid file (%s)", PID_FILE);

	pidFilehandle = open(PID_FILE, O_RDWR|O_CREAT, 0644);

	/* Couldn't open/create pid file */
	if (pidFilehandle == -1 )
	{
		syslog(LOG_INFO, "Could not create & open pid file %s. Fatal.", PID_FILE);
		fprintf(stderr, "Could not create & open pid file %s. Fatal.", PID_FILE);
		exit(EXIT_FAILURE);
	}

	syslog(LOG_INFO, "Locking pid file", PID_FILE);

	/* Couldn't get lock on pid file */
	if ( lockf(pidFilehandle, F_TLOCK | F_UNLCK, 0) == -1 )
	{
		syslog(LOG_INFO, "Couldn't lock pid file. Fatal. ");
		fprintf(stderr, "Couldn't lock pid file. Fatal. ");
		exit(EXIT_FAILURE);
	}

	sprintf(pidstr, "%d\n", getpid()); /* Get and format pid */

	syslog(LOG_INFO, "Writing process ID to pid file (%d)", getpid());

	write(pidFilehandle, pidstr, strlen(pidstr)); /* Write pid to lockfile */

	syslog(LOG_INFO, "Daemon initialization complete.");
}

int main(int argc, char *argv[])
{
	int i, DebugLevel, pretval, Slot_Channels[IO_SLOTS], PollCount = 0, RF_InputChan = ( SIMULATE_CHANNEL >= 0 ? 0 : -1 );
	char uri[200], szSend[80], szReceive[80], pidstr[10];
	float fBuf[12];
	DWORD Buffer[12], dwModuleName;
	GPIO_Inputs slots[IO_SLOTS];
	RFAlert_Config rf_config;
	pthread_t pthread1;

	/* Log to syslog until we're in daemon mode and have initialized our own logging agent */
	setlogmask (LOG_UPTO (LOG_NOTICE));
	openlog(DAEMON_NAME, LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1);

	syslog(LOG_NOTICE, "%s starting (UID: %d)", DAEMON_NAME, getuid());

	if ( getuid() != 0 )
	{
		syslog(LOG_NOTICE, "%s requires root privileges to run. Please restart under sudo or root user.", DAEMON_NAME);
		fprintf(stderr, "%s requires root privileges to run. Please restart under sudo or root user. \n", DAEMON_NAME);

		exit(EXIT_FAILURE);
	}

	daemonize();

	syslog(LOG_NOTICE, "Initializing logging agent (%s)", LOG_FILE);

	/* Initialize logging agent */
	if ( ( init_log(LOG_FILE) ) )
	{
		syslog(LOG_NOTICE, "Error initializing logging agent. Fatal.");
		exit(EXIT_FAILURE);
	}

	/* Done with syslog */
	syslog(LOG_NOTICE, "Redirecting %s log output to %s. %s syslog complete.", DAEMON_NAME, LOG_FILE, DAEMON_NAME);
	closelog();

	LOG("Reading WatchmanAlerting configuration file (%s)", E_WARN, CONFIG_FILE);

	if ( ini_parse(CONFIG_FILE, ini_handler, &ini_config) < 0 )
	{
		LOG("Can't load system configuration file (%s) ", E_CRIT, CONFIG_FILE);
		exit(EXIT_FAILURE);
	}

	DEBUG = ini_config.sysdebug;
	DebugLevel = ini_config.debuglevel;

	if (DEBUG && DebugLevel == 0)
		DebugLevel = 1;

    if (DEBUG) LOG("Debug output enabled (level %d)", E_WARN, DebugLevel);

    LOG("Initiating database connection pool", E_WARN);

    snprintf(uri, 200, "mysql://%s:%d/%s?user=%s&password=%s", ini_config.dbhost, ini_config.dbport, ini_config.dbname, ini_config.dbuser, ini_config.dbpass);

    if (DEBUG) LOG("Connecting to database URI: [%s]", E_WARN, uri);

    URL_T url = URL_new(uri);
    if (url == NULL)
    {
    	LOG("Error initializing database URL parser (%s)", E_CRIT, uri);
    	return -1;
    }

    pool = ConnectionPool_new(url);
	if (pool == NULL)
	{
		LOG("Error initializing database connection pool.", E_CRIT);
		return -1;
	}

	ConnectionPool_setReaper(pool, 360);
	ConnectionPool_start(pool);

	/* AMQP Queue Objects */
	amqp_socket_t *amqp_socket = NULL;
	amqp_connection_state_t amqp_conn;

	LOG("Initiating AMQP connection", E_WARN);

	/* Initiate connection to the AMQP queuing service */
	if ( init_amqp(&amqp_socket, &amqp_conn) )
	{
		LOG("Exceptions received during AMQP initialization. Exiting.", E_CRIT);
		exit(EXIT_FAILURE);
	}

	/* Load RF_Alert configuration settings from Settings database */
	if ( load_rfconfig(&rf_config) )
	{
		LOG("Exceptions received while loading RF alerting configuration from system settings database. Exiting.", E_CRIT);
		exit(EXIT_FAILURE);
	}

	LOG("Initiating I/O slot connection", E_WARN);

	/* Initiate Slot I/O port */
#ifdef GPIO_SIMULATE
	LOG("** GPIO_SIMULATE - I/O slot initialization", E_WARN);
#else

	if ( init_slot() )
	{
		LOG("Exceptions received during I/O slot initialization. Exiting.", E_CRIT);
		exit(EXIT_FAILURE);
	}

#endif

	if ( rf_config.RF_Alerting_Enabled )
	{
		LOG("RF alerting is enabled", E_WARN);

		LOG("Loading slot/channel assignments for RF alerting", E_WARN);

		if ( load_slots(&slots, Slot_Channels) )
		{
			LOG("Exception received while loading RF alert slot/channel assignments. Exiting.", E_CRIT);
			exit(EXIT_FAILURE);
		}

		LOG("Setting RF alert poll interval => [%d] and Activewait time => [%d] ", E_WARN, rf_config.Poll_Interval, rf_config.ActiveWait);

		if (DEBUG)
		{
			int _i;
			char __buff[500];

			LOG("Exporting GPIO mapping for RF Alerting (%d I/O slots) ", E_DEBUG, (int)IO_SLOTS);

			for ( _i = 0; _i < IO_SLOTS; _i++ )
			{
				sprintf(__buff, "GPIO Slot Assignment %d: ", _i);

				if (slots[_i].IO_Slot > 0)
				{
					sprintf(__buff, "%sEnabled on Slot %d (%s) with %d input channels (", __buff, slots[_i].IO_Slot, slots[_i].IO_Device, slots[_i].Channel_Count);

					int _j;

					for (_j = 0; _j < Slot_Channels[_i]; _j++ )
					{
						if (slots[_i].IO_Channel[_j] > 0)
						{
							sprintf(__buff, "%s%d", __buff, slots[_i].IO_Channel[_j]);

							if (_j < Slot_Channels[_i]-1 && slots[_i].IO_Channel[_j+1] > 0)
								sprintf(__buff, "%s, ", __buff);
						}

					}

					sprintf(__buff, "%s)", __buff);
				}
				else
					sprintf(__buff, "%sDisabled", __buff);

				LOG("%s", E_DEBUG, __buff);
				__buff[0] = '\0';
			}
		}
	}
	else LOG("RF alerting is disabled", E_WARN);

	LOG("Initializing pthread mutex for shared lock", E_WARN);

	if ( pthread_mutex_init(&SlotLock.SlotInUse, NULL) != 0 )
	{
		LOG("Error initializing mutex lock ", E_CRIT);
		exit(EXIT_FAILURE);
	}

	LOG("Spawning IO_Device polling thread", E_WARN);

	pretval = pthread_create( &pthread1, NULL, poll_devicequeue, (void *) &slots);
	if ( pretval != 0 )
	{
		LOG("Error spawning pthread for device IO polling: %s", E_CRIT, strerror(pretval));
		exit(EXIT_FAILURE);
	}

	int activate, wRetVal;
	int RF_Simulate = 0;

	LOG("Starting processing for GPIO event polling", E_WARN);

	while ( ! CONTINUE )
	{
		nanosleep((struct timespec[]){{0, (rf_config.Poll_Interval * 1000000)}}, NULL);

		/* If RF alerting is enabled then start polling for tone alerting activations */
		if ( rf_config.RF_Alerting_Enabled )
		{
			activate = 0;

			for ( i = 0; i < IO_SLOTS; i++ )
			{
				if (slots[i].IO_Slot > 0 && slots[i].Channel_Count > 0)
				{
					pthread_mutex_lock(&SlotLock.SlotInUse);

					if (SlotLock.ActiveSlot != slots[i].IO_Slot)
					{
						if (DEBUG) LOG("Active slot mismatch (%d) - Changing to slot %d", SlotLock.ActiveSlot, slots[i].IO_Slot);

#ifdef GPIO_SIMULATE
						LOG("** GPIO_SIMULATE - Changing active slot (%d)", E_WARN, slots[i].IO_Slot);
#else
						ChangeToSlot(slots[i].IO_Slot);
#endif

						SlotLock.ActiveSlot = slots[i].IO_Slot;
					}

				    dwModuleName = slots[i].IO_Device[2]-'0';
				    dwModuleName *= 16;
				    dwModuleName += slots[i].IO_Device[3]-'0';
				    dwModuleName *= 16;
				    dwModuleName += slots[i].IO_Device[4]-'0';
				    dwModuleName *= 16;
				    dwModuleName += slots[i].IO_Device[5]-'0';

				    if (slots[i].IO_Device[3] == '7')
				    {
				        dwModuleName *= 16;
				        dwModuleName += slots[i].IO_Device[6]-'0';
				    }

					Buffer[0] = COM1; 			// COM Port
					Buffer[1] = 00; 			// Address
					Buffer[2] = dwModuleName; 	// Device Model ID
					Buffer[3] = 0; 				// CheckSum disable
					Buffer[4] = 100; 			// TimeOut , 100 msecond
					Buffer[6] = 0; 				// Save String

#ifdef GPIO_SIMULATE
					LOG("** GPIO_SIMULATE (%d) - Calling DigitalIn_87K() function ", E_WARN, RF_Simulate);
					wRetVal = 0;

					if ( RF_InputChan = 0 )
						RF_InputChan = _rand(1, 4);


					if ( RF_InputChan > 0 && RF_Simulate % 50 <= 5 )
					{
						LOG("** GPIO_SIMULATE - Setting random RF input activation (%d) **", E_WARN, RF_InputChan);
						Buffer[5] = (int)RF_InputChan;
					}
					else
						Buffer[5] = 0;

					if ( RF_InputChan >= 0 )
						RF_InputChan = 0;

					RF_Simulate++;
					if ( RF_Simulate == 50 )
						RF_Simulate = 0;
#else

				    wRetVal = DigitalIn_87K(Buffer, fBuf, szSend, szReceive);

				    if ( ( DEBUG && DebugLevel > 1 ) || ( DEBUG && PollCount % 10 == 0 )) LOG("Slot %d activation scan #%d (RetVal %d): [%u]", E_DEBUG, SlotLock.ActiveSlot, PollCount, wRetVal, Buffer[5]);
#endif

					if (wRetVal)
					{
						LOG("Error reading GPIO input status on slot %d (Err %d)", E_ERROR, SlotLock.ActiveSlot, wRetVal);
						ChangeToSlot(slots[i].IO_Slot);
					}
					else
					{
						if (Buffer[5] != 0 )
						{
							char buff[ slots[i].Device_Inputs ];
							hexmap(buff, Buffer[5], slots[i].Device_Inputs);

							LOG("RF activation received on slot [%d] device input channels [%s]", E_WARN, SlotLock.ActiveSlot, buff);

							int j, k;

							for (j = 0; j < slots[i].Device_Inputs; j++ )
							{
								int devchan = slots[i].Device_Inputs - j;

								if ( DEBUG )
									LOG("Checking input channel %d on slot %d => [%d]", E_DEBUG, devchan, SlotLock.ActiveSlot, ( buff[j] == '1' ? 1 : 0 ));

								if ( buff[j] == '1' )
								{
									int _map = -1;

									for ( k = 0; k < slots[i].Device_Inputs; k++ )
									{
										if ( slots[i].IO_Channel[k] > 0 && slots[i].IO_Channel[k] == devchan )
										{
											_map = k;

											if (DEBUG)
												LOG("Device channel [%d] mapped to RF channel [%d] ", E_DEBUG, devchan, slots[i].IO_Channel[_map] );

											break;
										}
									}

									if ( _map != -1 )
									{
										LOG("RF alert activation received on slot [%d] channel [%d]. Checking active wait time [%d] ", E_WARN, SlotLock.ActiveSlot, slots[i].IO_Channel[_map], slots[i].ActiveWait[_map].tv_sec);

										struct timeval timebuff;
										gettimeofday(&timebuff, NULL);

										if ( slots[i].ActiveWait[_map].tv_sec <= 0 || ( slots[i].ActiveWait[_map].tv_sec > 0 && timediff(NULL, &timebuff, &slots[i].ActiveWait[_map]) > rf_config.ActiveWait ) )
										{
											activate = 1;
											char amqp_buff[10];
											int retval, attempts = 0;

											gettimeofday(&slots[i].ActiveWait[_map], NULL);

											LOG("Active wait time is expired, setting activation flag for slot %d channel %d => [%d] ", E_WARN, SlotLock.ActiveSlot, slots[i].IO_Channel[_map], slots[i].ActiveWait[_map].tv_sec);

											sprintf(amqp_buff, "%d:%d", SlotLock.ActiveSlot, slots[i].IO_Channel[_map]);

											char const *amqp_msg = amqp_buff;

											/* Publish GPIO input status to rfalert queue */
											LOG("RF alert activation flag active - publishing activation message (%s) to AMQP server ", E_WARN, amqp_msg);

											amqp_basic_properties_t props;

											props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
											props.content_type = amqp_cstring_bytes("text/plain");
											props.delivery_mode = 2; /* Persistent delivery mode */
											props.expiration = amqp_cstring_bytes("5000");

											while ( ( retval = amqp_basic_publish(amqp_conn, 1, amqp_cstring_bytes(""), amqp_cstring_bytes(ini_config.amqp_rfalert_queue), 0, 0, &props, amqp_cstring_bytes(amqp_buff)) ) )
											{
												LOG("Error writing activation message [%s] to AMQP server [%s] (Attempt #%d): Err %d", E_CRIT, amqp_buff, ini_config.amqp_rfalert_queue, attempts++, retval);

												if ( attempts > 25 )
												{
													LOG("Unable to write activation message [%s] to AMQP server [%s] (Attempt #%d): Err %d", E_CRIT, amqp_buff, ini_config.amqp_rfalert_queue, attempts++, retval);
													break;
												}

												nanosleep((struct timespec[]){{0, (50 * 1000000)}}, NULL);
											}

											LOG("Activation message published to AMQP server (%d)", E_ERROR, retval);
										}
										else
											LOG("Active wait time for slot [%d] channel [%d] is pending at [%d ms] ", E_WARN, slots[i].IO_Slot, slots[i].IO_Channel[k], timediff(NULL, &timebuff, &slots[i].ActiveWait[_map]));
									}
									else
										LOG("Unable to map device input on slot [%d] channel [%d]. Check RF alert channel configuration", E_WARN, SlotLock.ActiveSlot, devchan);
								}
							}
						}
					}

					pthread_mutex_unlock(&SlotLock.SlotInUse);
				}
			}
		}

		PollCount++;
	}

	LOG("GPIO processing loop ended - Signaling active threaded processes => SIGTERM", E_WARN);

	pretval = pthread_kill(pthread1, SIGTERM);
	if ( pretval )
		LOG("Error killing threaded process (%d)", E_ERROR, pretval);

	LOG("Waiting on threaded process ...", E_WARN);

	pretval = pthread_cancel( pthread1 );
	LOG("Threaded process terminate with exit code (%d)", E_ERROR, pretval);

	pthread_mutex_destroy(&SlotLock.SlotInUse);

	LOG("Closing LinPAC I/O ports", E_WARN);
    shutdown_slot();

    LOG("Closing connection to AMQP server", E_WARN);
    shutdown_amqp(amqp_conn);

    /* Close and remove the PID file */
    LOG("Removing PID file", E_WARN);
    close(pidFilehandle);
    unlink(PID_FILE);

   	LOG("%s shutdown complete..", E_DEBUG, DAEMON_NAME);

    exit(EXIT_SUCCESS);
}

/* Poll the AMQP IO device queue in seperate thread for new device requests */
void* poll_devicequeue(void* s)
{
	GPIO_Inputs* slots = (GPIO_Inputs*)s;
	float fBuf[12];
	int PollCount = 0;

	/* AMQP Queue Objects */
	amqp_socket_t *amqp_socket = NULL;
	amqp_connection_state_t amqp_conn;

	LOG("Initiating AMQP connection for IO_Device requests", E_WARN);

	/* Initiate connection to the AMQP queuing service */
	if ( init_amqp(&amqp_socket, &amqp_conn) )
	{
		LOG("Exceptions received during AMQP initialization. Exiting.", E_CRIT);
		return (void*)NULL;
	}

	LOG("Binding AMQP consumer to queue [%s]", E_WARN, ini_config.amqp_iodevice_queue);

	amqp_basic_consume(amqp_conn, 1, amqp_cstring_bytes(ini_config.amqp_iodevice_queue), amqp_empty_bytes, 0, 0, 0, amqp_empty_table);

	while ( 1 )
	{
		/* Check the amqp queue for GPIO device requests */
		amqp_rpc_reply_t res;
		amqp_envelope_t envelope;

		amqp_maybe_release_buffers(amqp_conn);

		res = amqp_consume_message(amqp_conn, &envelope, NULL, 0);

		if (AMQP_RESPONSE_NORMAL != res.reply_type)
		{
			LOG("AMQP consume message [%s]: Invalid response (%d) ", E_ERROR, ini_config.amqp_iodevice_queue, res.reply_type);
			continue;
		}

		if (DEBUG)
			LOG("AMQP consume message [%s]: Response (%d)", E_DEBUG, ini_config.amqp_iodevice_queue, res.reply_type);

		if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG)
		{
			uint64_t delivery_tag = envelope.delivery_tag;
			char *iomsg = envelope.message.body.bytes;
			int i, j, k, slot, chan, val, _slot, retval;

			iomsg[envelope.message.body.len] = '\0';

			LOG("New request (%d) received on queue [%s]: Request: (%s) [%s]", E_WARN, PollCount++, ini_config.amqp_iodevice_queue, (char *)envelope.message.properties.content_type.bytes, iomsg);

			if ( amqp_basic_ack(amqp_conn, 1, delivery_tag, 0) )
				LOG("Error acknowledging AMQP message [%u]\n", E_ERROR, (int)delivery_tag);

			sscanf (iomsg, "%d:%d=%d", &slot, &chan, &val);

			if ( slot > IO_SLOTS )
			{
				LOG("IO Device Request Error: Requested slot (%d) exceeds available devices (%d)", E_ERROR, slot, IO_SLOTS);
				continue;
			}

			_slot = 1;

			for ( k = 0; k < IO_SLOTS; k++ )
			{
				if ( DEBUG ) LOG("Checking IO device map #%d for requested slot [%d] => [%d]", E_DEBUG, k, slot, slots[k].IO_Slot);

				if ( slot == slots[k].IO_Slot )
				{
					pthread_mutex_lock(&SlotLock.SlotInUse);

					_slot = 1; // Valid slot number

	        		if ( chan > slots[k].Device_Outputs )
	        			LOG("Invalid channel assignment (%d) for slot %d, exceeds max channel count (%d)", E_ERROR, chan, slots[k].IO_Slot, slots[k].Device_Outputs);
	        		else
	        		{
						if (SlotLock.ActiveSlot != slots[k].IO_Slot)
						{
							if (DEBUG) LOG("Active slot mismatch (%d) - Changing to slot %d", SlotLock.ActiveSlot, slots[k].IO_Slot);

#ifdef GPIO_SIMULATE
							LOG("** GPIO_SIMULATE - Changing active slot (%d)", E_WARN, slots[k].IO_Slot);
#else
							ChangeToSlot(slots[k].IO_Slot);
#endif

							SlotLock.ActiveSlot = slots[k].IO_Slot;
						}

						DWORD dwBuff[12], dwModuleName;
						static char szSend[80], szReceive[80];

					    dwModuleName = slots[k].IO_Device[2]-'0';
					    dwModuleName *= 16;
					    dwModuleName += slots[k].IO_Device[3]-'0';
					    dwModuleName *= 16;
					    dwModuleName += slots[k].IO_Device[4]-'0';
					    dwModuleName *= 16;
					    dwModuleName += slots[k].IO_Device[5]-'0';

					    if (slots[k].IO_Device[3] == '7')
					    {
					        dwModuleName *= 16;
					        dwModuleName += slots[k].IO_Device[k]-'0';
					    }

						dwBuff[0] = 1; 				// COM Port
						dwBuff[1] = 00; 				// Address
						dwBuff[2] = 0x87063; //dwModuleName; 		// Device Model ID
						dwBuff[3] = 0; 					// CheckSum disable
						dwBuff[4] = 100; 				// TimeOut , 100 msecond
						dwBuff[5] = (val == 1 ? 1 : 0);	// On/Off
						dwBuff[6] = 0;					// Save string
						dwBuff[7] = chan;				// Output channel TODO: Might need to decrement channel number (i.e. Channel 1 is actually channel 0)
						dwBuff[8] = (val == 1 ? 1 : 0);	// On/Off

#ifdef GPIO_SIMULATE
						LOG("** GPIO_SIMULATE - Setting output channel %d=>%d on slot %d", E_WARN, dwBuff[7], dwBuff[8], SlotLock.ActiveSlot);
						retval = 0;
#else
						int count = 0;

						while ( ( retval = DigitalBitOut_87K(dwBuff, fBuf, szSend, szReceive) ) )
						{
							LOG("Error writing output %d on slot %d (Attempt #%d): Err %d", E_ERROR, chan, SlotLock.ActiveSlot, count++, retval);

							if ( count > 10 )
							{
								LOG("Unable to write output %d on slot %d after %d attempts. Giving up with error %d", E_ERROR, chan, SlotLock.ActiveSlot, count, retval);
								break;
							}

							nanosleep((struct timespec[]){{0, (250 * 1000000)}}, NULL);
						}
#endif
	        		}

	        		pthread_mutex_unlock(&SlotLock.SlotInUse);

	        		break;
				}
			}

			if ( _slot == -1 )
				LOG("Invalid slot assignment: IO device mapping not found for slot %d", E_ERROR, slot);
		}

		amqp_destroy_envelope(&envelope);
	}

	LOG("Calling shut down functions for IO_Device polling agent", E_WARN);

    shutdown_amqp(amqp_conn);

    LOG("IO_Device polling agent has terminated successfully", E_WARN);

	return 0;
}

int load_rfconfig(void* s)
{
	RFAlert_Config* pconfig = (RFAlert_Config*)s;
	ResultSet_T r;

	LOG("Loading system settings for RF alerting", E_WARN);

	Connection_T con = ConnectionPool_getConnection(pool);
	if (con == NULL)
	{
		LOG("Database connection request failed - Total connections exceeds allowed maximum.\n", E_WARN);
		return 1;
	}

	TRY
		r = Connection_executeQuery(con, "SELECT Name, CAST( Setting AS UNSIGNED INTEGER) FROM Settings WHERE Category = 'rfalert'");
	CATCH(SQLException)
		LOG("Database Exception: %s [%s]", E_ERROR, Connection_getLastError(con), "SELECT Name, Setting FROM Settings WHERE Category = 'rfalert'");
		return 1;
	END_TRY;

	while ( ResultSet_next(r) )
	{
		if (DEBUG) LOG("RF alert setting [%s] => [%d]", E_DEBUG, ResultSet_getString(r, 1), ResultSet_getInt(r, 2));

		if ( strcmp(ResultSet_getString(r, 1), "RF_Alerting_Enabled") == 0 ) pconfig->RF_Alerting_Enabled = ResultSet_getInt(r, 2);
		else if ( strcmp(ResultSet_getString(r, 1), "Poll_Interval") == 0 ) pconfig->Poll_Interval = ResultSet_getInt(r, 2);
		else if ( strcmp(ResultSet_getString(r, 1), "ActiveWait") == 0 ) pconfig->ActiveWait = ResultSet_getInt(r, 2);
		else if ( strcmp(ResultSet_getString(r, 1), "RF_LED_Enabled") == 0 ) pconfig->RF_LED_Enabled = ResultSet_getInt(r, 2);
		else if ( strcmp(ResultSet_getString(r, 1), "RF_LEDResetTime") == 0 ) pconfig->RF_LEDResetTime = ResultSet_getInt(r, 2);
	}

	if (pconfig->Poll_Interval <= 500 )
	{
		LOG("RF alerting poll interval (%d) is unset or too low - Overriding", E_WARN, pconfig->Poll_Interval);
		pconfig->Poll_Interval = 500;
	}

	if ( pconfig->ActiveWait <= 0 )
	{
		LOG("Activewait time (%d) is unset or too low - Overriding", E_WARN, pconfig->ActiveWait);
		pconfig->ActiveWait = 2500;
	}

    if ( con )
    {
		Connection_clear(con);
		Connection_close(con);
    }

	return 0;
}


int load_slots(void* s, int *ch_count)
{
	GPIO_Inputs* pconfig = (GPIO_Inputs*)s;
	ResultSet_T r, r2;
	int i, j;

	if (DEBUG) LOG("Loading RF Alert slot assignments from RF_Alert configuration ", E_WARN);

	Connection_T con = ConnectionPool_getConnection(pool);
	if (con == NULL)
	{
		LOG("Connection request failed - Total connections exceeds allowed maximum", E_CRIT);
		return 1;
	}

	TRY
		r = Connection_executeQuery(con, "SELECT IO_Slot, CONCAT('0x', IO_Device) AS IO_Device  FROM RF_Alert GROUP BY IO_Slot ORDER BY IO_Slot ASC LIMIT 7");
	CATCH(SQLException)
		LOG("Database Exception: %s", E_CRIT, Connection_getLastError(con));
		return 1;
	END_TRY;

	/* Initialize GPIO_Input struct members */
	for ( i = 0; i < IO_SLOTS; i++ )
	{
		pconfig[i].IO_Slot = 0;
		pconfig[i].Channel_Count = 0;
		pconfig[i].Device_Inputs = 0;
		pconfig[i].Device_Outputs = 0;

		ch_count[i] = 0;
	}

	i = 0;

	while ( ResultSet_next(r) )
	{
		int _slot = ResultSet_getInt(r, 1);

		pconfig[i].IO_Slot = _slot;
		pconfig[i].IO_Device = strdup(ResultSet_getString(r, 2));

		if ( strcmp(pconfig[i].IO_Device, "0x87063") == 0 )
		{
			pconfig[i].Device_Inputs = IO_87063_INPUTS;
			pconfig[i].Device_Outputs = IO_87063_OUTPUTS;
		}

		/* Initialize input channels on the IO device */
		ch_count[i] = pconfig[i].Device_Inputs;
		pconfig[i].IO_Channel[ pconfig[i].Device_Inputs ] = '\0';

		for ( j = 0; j < pconfig[i].Device_Inputs; j++ )
		{
			pconfig[i].IO_Channel[j] = 0;
			pconfig[i].ActiveWait[j].tv_sec = 0;
			pconfig[i].ActiveWait[j].tv_usec = 0;
		}

		LOG("Registering IO device [%s] on slot %d for RF alerting (%d Inputs/%d Outputs)", E_WARN, pconfig[i].IO_Device, pconfig[i].IO_Slot, pconfig[i].Device_Inputs, pconfig[i].Device_Outputs);

		i++;
	}

	for ( i = 0; i < IO_SLOTS; i++ )
	{
		if (pconfig[i].IO_Slot > 0 )
		{
			TRY
				r = Connection_executeQuery(con, "SELECT IO_Channel FROM RF_Alert WHERE IO_Slot = %d", pconfig[i].IO_Slot);
			CATCH(SQLException)
				LOG("Error loading IO channels for slot %d: %s", pconfig[i].IO_Slot, Connection_getLastError(con));
			END_TRY;

			int j = 0;

			while ( ResultSet_next(r) )
			{
				/* TODO: Add conditional check to ensure database doesn't try to load more channels than are available */
				pconfig[i].IO_Channel[j] = ResultSet_getInt(r, 1);
				j++;
			}

			pconfig[i].Channel_Count = j;

			LOG("Setting RF alerting active channel count for slot %d: [%d]", E_WARN, pconfig[i].IO_Slot, pconfig[i].Channel_Count);
		}
	}

    if ( con )
    {
		Connection_clear(con);
		Connection_close(con);
    }

	return 0;
}

int init_slot(void)
{
	int retval;

	retval = Open_Slot(0);
	if (retval > 0)
	{
		LOG("Failed to open slot (%d). Device not accessible on slot 0. ", E_WARN, retval);
		return (-1);
	}

	retval = Open_Com(COM1, 115200, Data8Bit, NonParity, OneStopBit);
	if (retval > 0)
	{
		LOG("Failed to open I/O slot communications port on COM1 (%d) ", E_WARN, retval);
		return (-1);
	}

	return 0;
}

int init_amqp(amqp_socket_t **sock, amqp_connection_state_t *conn)
{

	if ( ini_config.amqp_host == NULL || ini_config.amqp_port == 0 || ini_config.amqp_rfalert_queue == NULL )
	{
		char _b[250];

		sprintf(_b, "Invalid AMQP configuration. Incomplete or invalid values found in parameters: [");
		if (ini_config.amqp_host == NULL ) sprintf(_b, "%samqp_host ", _b);
		if (ini_config.amqp_port == 0 ) sprintf(_b, "%samqp_port ", _b);
		if (ini_config.amqp_rfalert_queue == NULL ) sprintf(_b, "%samqp_rfalert_queue ", _b);
		sprintf(_b, "%s]", _b);

		LOG("%s", E_CRIT, _b);

		return 1;
	}

	/* Initiate the AMQP queuing service */
	*conn = amqp_new_connection();

	*sock = amqp_tcp_socket_new(*conn);
	if ( ! *sock )
	{
		LOG("Error initiating new TCP socket for AMQP service.", E_CRIT);
		return 1;
	}

	LOG("Opening AMQP socket to %s on port %d", E_WARN, (char*)ini_config.amqp_host, (int)ini_config.amqp_port);

	if ( amqp_socket_open(*sock, ini_config.amqp_host, ini_config.amqp_port) )
	{
		LOG("Error opening AMQP socket. Can't connect to TCP socket.", E_CRIT);
		return 1;
	}

	if ( AMQP_LOG( amqp_login(*conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"), "Logging in") < 0 )
		return 1;

	LOG("Opening AMQP channel connection", E_WARN);

	amqp_channel_open(*conn, 1);
	if ( AMQP_LOG(amqp_get_rpc_reply(*conn), "Opening channel") < 0 )
		return 1;

	return 0;
}

void shutdown_slot(void)
{
	LOG("Closing IO/Slot COM connection", E_WARN);

	Close_Com(COM1);
	Close_SlotAll();
}

void shutdown_amqp(void *conn)
{
	amqp_rpc_reply_t retval;

	LOG("Closing AMQP server channel", E_WARN);
	retval = amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);

	if ( retval.reply_type != AMQP_RESPONSE_NORMAL )
		LOG("AMQP server channel error: %d", E_ERROR, retval.reply_type);

	LOG("Closing AMQP server connection", E_WARN);
	retval = amqp_connection_close(conn, AMQP_REPLY_SUCCESS);;

	if ( retval.reply_type != AMQP_RESPONSE_NORMAL )
		LOG("AMQP server connection error: %d", E_ERROR, retval.reply_type);

	amqp_destroy_connection(conn);
}

int ini_handler(void* user, const char* section, const char* name, const char* value)
{
	IniConfig* pconfig = (IniConfig*)user;

    #define MATCH(s, n) strcmp(section, s) == 0 && strcmp(name, n) == 0

    if (MATCH("database", "db_name")) pconfig->dbname = strdup(value);
    else if (MATCH("database", "user")) pconfig->dbuser = strdup(value);
    else if (MATCH("database", "pass")) pconfig->dbpass = strdup(value);
    else if (MATCH("database", "host")) pconfig->dbhost= strdup(value);
    else if (MATCH("database", "port")) pconfig->dbport = atoi(value);
    else if (MATCH("database", "socket") && value) pconfig->dbsocket = strdup(value);
    else if (MATCH("system", "debug")) pconfig->sysdebug = atoi(value);
    else if (MATCH("system", "debug_level")) pconfig->debuglevel = atoi(value);
    else if (MATCH("amqp_queue", "host")) pconfig->amqp_host = strdup(value);
    else if (MATCH("amqp_queue", "port")) pconfig->amqp_port = atoi(value);

    else if (MATCH("amqp_queue", "iodevice_enabled")) pconfig->amqp_iodevice_enabled = atoi(value);
    else if (MATCH("amqp_queue", "iodevice_queue")) pconfig->amqp_iodevice_queue = strdup(value);
    else if (MATCH("amqp_queue", "iodevice_exchange")) pconfig->amqp_iodevice_exchange = strdup(value);
    else if (MATCH("amqp_queue", "rfalert_queue")) pconfig->amqp_rfalert_queue = strdup(value);
    else if (MATCH("amqp_queue", "rfalert_exchange")) pconfig->amqp_rfalert_exchange = strdup(value);
    else if (MATCH("gpio", "sim_mode")) pconfig->gpio_simulation = atoi(value);
    else return 0;  /* unknown section/name, error */

    return 1;
}

void hexmap(char *buff, uint32_t hex, int in_size)
{
	int i;

	LOG("Mapping hex value [0x%d]", E_WARN, hex);

	/* Normally either 4 or 8 input channels */
	buff[in_size] = '\0';

	LOG("Initializing bitwise storage for %d input channels", E_WARN, in_size);

	for ( i = 0; i < in_size; i++ )
		buff[0] = '0';

	switch( hex )
	{
		case '0':
		case 0:
			sprintf(buff, "0000");
			break;
		case '1':
		case 1:
			sprintf(buff, "0001");
			break;
		case '2':
		case 2:
			sprintf(buff, "0010");
			break;
		case '3':
		case 3:
			sprintf(buff, "0011");
			break;
		case '4':
		case 4:
			sprintf(buff, "0100");
			break;
		case '5':
		case 5:
			sprintf(buff, "0101");
			break;
		case '6':
		case 6:
			sprintf(buff, "0110");
			break;
		case '7':
		case 7:
			sprintf(buff, "0111");
			break;
		case '8':
		case 8:
			sprintf(buff, "1000");
			break;
		case '9':
		case 9:
			sprintf(buff, "1001");
			break;
		case 'A':
		case 'a':
			sprintf(buff, "1010");
			break;
		case 'B':
		case 'b':
			sprintf(buff, "1011");
			break;
		case 'C':
		case 'c':
			sprintf(buff, "1100");
			break;
		case 'D':
		case 'd':
			sprintf(buff, "1101");
			break;
		case 'E':
		case 'e':
			sprintf(buff, "1110");
			break;
		case 'F':
		case 'f':
			sprintf(buff, "1111");
			break;
		default:
			sprintf(buff, "0000");
			break;
	}

	//sprintf(buff, "%s", strrev(buff));
	return;
}


long long timediff (struct timeval *difference, struct timeval *end_time, struct timeval *start_time )
{
	struct timeval temp_diff;

	if(difference==NULL)
	{
		difference=&temp_diff;
	}

	difference->tv_sec =end_time->tv_sec -start_time->tv_sec ;
	difference->tv_usec=end_time->tv_usec-start_time->tv_usec;

	/* Using while instead of if below makes the code slightly more robust. */

	while(difference->tv_usec<0)
	{
		difference->tv_usec+=1000000;
		difference->tv_sec -=1;
	}

	return ( ( 1000000LL*difference->tv_sec+difference->tv_usec ) / 1000 );

} /* timeval_diff() */

char *strrev(char *str)
{
	char *p1, *p2;

	if (! str || ! *str)
		return str;
	for (p1 = str, p2 = str + strlen(str) - 1; p2 > p1; ++p1, --p2)
	{
		*p1 ^= *p2;
		*p2 ^= *p1;
		*p1 ^= *p2;
	}
	return str;
}












