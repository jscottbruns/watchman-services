#ifndef GPIO_SERVER_H_
#define GPIO_SERVER_H_

#define DAEMON_NAME "gpio-server"
#define PID_FILE "/var/run/gpio-server.pid"
#define LOG_FILE "/var/log/watchman-alerting/gpio-server.log"
#define CONFIG_FILE "/etc/watchman.conf"

//
//	LinPAC Controller Model
//
#define LINPAC_MODEL 8381
//#define LINPAC_MODEL 8781

//
// Slot I/O device mapping
//
#define IO_87063_INPUTS 4
#define IO_87063_OUTPUTS 4

//
// GPIO simulation mode - for testing on non-LinPAC platforms
//
//#define GPIO_SIMULATE 1
#define SIMULATE_CHANNEL -1

#if LINPAC_MODEL == 8381
#define IO_SLOTS 3
#elif LINPAC_MODEL == 8781
#define IO_SLOTS 7
#endif

typedef struct
{
	const char* IO_Device; // Device Model (i.e. 0x87042)
	int IO_Slot; 		// Slot No
	int IO_Channel[25];		// Channel No
	struct timeval ActiveWait[25];
	int Channel_Count;
	int Device_Inputs;
	int Device_Outputs;
} GPIO_Inputs;

typedef struct
{
	// Database Config
	const char* dbname;
    const char* dbuser;
    const char* dbpass;
    const char* dbhost;
    int dbport;
    const char* dbsocket;

    // System Config
    int sysdebug;
    int debuglevel;
    const char* syspidfile;

    // Amqp Config
    const char* amqp_host;
    int amqp_port;
    const char* amqp_rfalert_queue;
    const char* amqp_rfalert_exchange;
    int amqp_iodevice_enabled;
    const char* amqp_iodevice_queue;
    const char* amqp_iodevice_exchange;

    // GPIO
    int gpio_simulation;
} IniConfig;

typedef struct
{
	int RF_Alerting_Enabled;
    int Poll_Interval;
    int ActiveWait;
    int RF_LED_Enabled;
    int RF_LEDResetTime;
} RFAlert_Config;

extern void signal_handler(int);
extern void shutdown_slot(void);
void shutdown_amqp(void *conn);
extern int init_slot(void);
extern int init_amqp(amqp_socket_t **sock, amqp_connection_state_t *conn);
extern int ini_handler(void*, const char* section, const char* name, const char* value);
extern int load_rfconfig(void*);
extern int load_slots(void* s, int *ch_count);
extern void hexmap(char* buff, uint32_t hex, int in_size);
extern char *strrev(char *str);
extern long long timediff (struct timeval *difference, struct timeval *end_time, struct timeval *start_time );
//extern int parse_iorequest(char *reqstr, int count[IO_SLOTS], unsigned char (*iodev)[25]);
void* poll_devicequeue(void* s);
void daemonize();

extern ConnectionPool_T pool;
extern IniConfig ini_config;
extern int DEBUG;
extern int SlotInUse;

#endif /* GPIO_SERVER_H_ */
