/*
 * Copyright © 2009-2010 Stéphane Raimbault <stephane.raimbault@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <ctype.h>

#include <modbus.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <math.h>

#include "include/alertzone-server.h"
#include <zdb.h>

#define NB_CONNECTION	50
#define BUFF_LEN		255
#define REMOTE_PORT		49950

int DEBUG = 0;

modbus_t *ctx = NULL;
modbus_t *ctx2 = NULL;
int server_socket;
int ascii_socket;
modbus_mapping_t *mb_mapping;

URL_T url;
ConnectionPool_T pool;

void int2Ipaddr(int);
int ipToInt(int, int, int, int);

static void close_sigint(int dummy)
{
    close(server_socket);
    modbus_free(ctx);
    //modbus_free(ctx2);
    modbus_mapping_free(mb_mapping);

    exit(dummy);
}

char* substring(const char* str, size_t begin, size_t len)
{
  if (str == 0 || strlen(str) == 0 || strlen(str) < begin || strlen(str) < (begin+len))
    return 0;

  return strndup(str + begin, len);
}


int zone_init(char **chunkp, int l, char **__ip, char * variable)
{
	int i;
	*chunkp = malloc(l * sizeof **chunkp);

	if ( DEBUG )
		printf("Client: [%s] Zone INIT configuration request\n", *__ip);

	if (*chunkp == NULL) {
		printf("Malloc error\n");
		return -2;
	}

	Connection_T con = ConnectionPool_getConnection(pool);

	if ( ! con )
	{
		printf("Error accessing database pool\n");
		return -1;
	}

	if (variable == "")
	{
		printf("Not Found\n");
		Connection_clear(con);
		Connection_close(con);
		return -1;
	}

	if(DEBUG)
		printf("  INET Inqury (%s)\n", variable);

	if ( strcmp(variable, "01") == 0 || strcmp(variable, "02") == 0 )
	{

		ResultSet_T r = Connection_executeQuery(
			con,
		    "SELECT ZoneID, ZoneName, iFaceHostAddr, DefaultVolLevel, AlertVolLevel, SilentWatch, SilentStartTime, SilentEndTime, SilentVolLevel "
		    "FROM AlertZones "
		    "WHERE HostAddr = '%s'",
		    *__ip
		);

		if ( ResultSet_next(r) )
		{
			if ( strcmp(variable, "01") == 0 )
				*chunkp = ResultSet_getStringByName(r, "ZoneID");
			else if ( strcmp(variable, "02") == 0 )
				*chunkp = ResultSet_getStringByName(r, "ZoneName");
		}
		else
			*chunkp = "-1";

	    if(DEBUG)
	    	printf("  Response: [%s]\n", *chunkp);

		return strlen(*chunkp);

	}
	else if ( strcmp(variable, "03") == 0 )
	{

		ResultSet_T r = Connection_executeQuery(
			con,
		    "SELECT t2.GroupName "
		    "FROM AlertGroupMember t1 "
		    "LEFT JOIN AlertGroups t2 ON t2.GroupAddr = t1.GroupAddr "
		    "LEFT JOIN AlertZones t3 ON t3.ZoneID = t1.ZoneID "
		    "WHERE t3.HostAddr = '%s'",
		    *__ip
		);

		char Units[150];

		while ( ResultSet_next(r) )
		{
			strcat(Units, (strlen(Units) > 0 ? "&": ""));
			strcat(Units, ResultSet_getStringByName(r, "GroupName"));

			printf("Returning [UME] configuration => %s \n", Units);
			sprintf(*chunkp, "%s", Units);

		    if(DEBUG)
		    	printf("  Response: [%s]\n", Units);

			return strlen(*chunkp);
		}

	    if ( con )
	    {
			Connection_clear(con);
			Connection_close(con);
	    }
	}

    if(DEBUG)
    	printf("  Response: [-1]\n");

	return -1;
}


/* Address Ranges:
Output Coils are assigned the block 1-9999
Input Coils are assigned the block 10001-19999
Input Coils are assigned the block 10001-19999
Input Register are assigned the block 30001-39999
*/

uint8_t modbus_get_coil_status(void *data, uint16_t address, char *__ipstr)
{
    if ( DEBUG )
    	printf("Client: [%s] Get coil status [%d] \n", __ipstr, address);

    return 0;
}

uint8_t modbus_get_input_status(void *data, uint16_t address, char *__ipstr)
{
	if ( DEBUG )
		printf("Client: [%s] Get input status [%d]\n", __ipstr, address);
    return 0;
}

// ModPoll Simulator: -t 3
uint16_t modbus_get_input_register(void *data, uint16_t address, char *__ipstr)
{
	if ( DEBUG )
		printf("Client: [%s] Get input register [%d]\n", __ipstr, address); /* Limited address range @30001-2*/
    return 0;
}

// ModPoll Simulator: -t 4
uint16_t modbus_get_holding_register(void *data, uint16_t address, char *__ipstr) // Most versatile for read/write settings
{
	if ( DEBUG )
		printf("Client: [%s] Get holding register [%d]\n", __ipstr, address);

	int i = 0;
	char *retval;

    Connection_T con = ConnectionPool_getConnection(pool);

    /*
     * Zone Activation Check
     */

    if ( address == 1 || address == 2 )
    {
    	if (DEBUG )
    		printf("  Zone Alert Inquiry (%d) \n", address);

		ResultSet_T r = Connection_executeQuery(
			con,
			"SELECT GROUP_CONCAT( t1.ObjID SEPARATOR '' ) AS Id, TIMESTAMPDIFF( MINUTE, t1.Timestamp, NOW() ) AS Elapsed "
			"FROM IncidentUnit t1 "
			"LEFT JOIN AlertGroups t2 ON t1.UnitID = t2.UnitID "
			"LEFT JOIN AlertGroupMember t3 ON t2.GroupAddr = t3.GroupAddr "
			"LEFT JOIN AlertZones t4 ON t3.ZoneID = t4.ZoneID "
			"WHERE t4.HostAddr = '%s' "
			"GROUP BY t1.IncidentNo "
			"UNION "
			"SELECT t1.ObjID AS Id, TIMESTAMPDIFF( MINUTE, t1.Timestamp, NOW() ) AS Elapsed "
			"FROM RF_Incident t1 "
			"LEFT JOIN AlertGroups t2 ON ( t2.GroupType = 1 AND t1.ToneId = t2.ToneID ) OR ( t2.GroupType = 0 AND t2.ToneID IS NULL ) "
			"LEFT JOIN AlertGroupMember t3 ON t2.GroupAddr = t3.GroupAddr "
			"LEFT JOIN AlertZones t4 ON t3.ZoneID = t4.ZoneID "
			"WHERE t4.HostAddr = '%s' "
			"ORDER BY Id DESC "
			"LIMIT 1",
			__ipstr,
			__ipstr
		);

		if ( ResultSet_next(r) )
		{
			// [AO0] ZONE_ACTIVE_ALERT
			if ( address == 1 ) i = ResultSet_getIntByName(r, "Id");

			// [AO1] ZONE_ALERT_ELAPSED
			else if ( address == 2 )
			{
				i = ResultSet_getIntByName(r, "Elapsed");
				if ( i > 1440 )
					i = 1440;
			}
		}
		//if ( address == 1 ) i = 500;
		//if ( address == 2 ) i = 600;
    }

    /*
     * Zone Settings & Prefs
     */
	else if ( ( address > 2 && address <= 20 ) || ( address >= 24 && address <= 26 ) )
	{
		char colname[64];
		char *__client = __ipstr;

		switch (address)
		{
			case 3: // [AO2] ZONE_IFACE_IPADDR
				strcpy(colname, "SUBSTRING_INDEX( iFaceHostAddr, '.', -1 ) ");
				break;
			case 4: // [AO3] ZONE_IFACE_TCPPORT
				strcpy(colname, "iFaceTcpControlPort");
				break;
			case 5: // [AO2] ZONE_ACTIVE_ALERT_TIMEOUT
				strcpy(colname, "TimeoutMins");
				break;
			case 6: // [AO2] ZONE_IO_RESET_ADDR
				strcpy(colname, "IoResetAddr");
				break;
			case 7: // [AO2] ZONE_DEFAULT_VOLUME
				strcpy(colname, "DefaultVolLevel");
				break;
			case 8: // [AO2] ZONE_ALERT_VOLUME
				strcpy(colname, "AlertVolLevel");
				break;
			case 9: // [AO2] ZONE_AUTO_UPDATE
				strcpy(colname, "AutoUpdate");
				break;
			case 10: // [AO2] ZONE_SILENT_WATCH
				strcpy(colname, "SilentWatch");
				break;
			case 11: // [AO2] ZONE_SILENT_WATCH_STARTTIME
				strcpy(colname, "SilentStartTime");
				break;
			case 12: // [AO2] ZONE_SILENT_WATCH_ENDTIME
				strcpy(colname, "SilentEndTime");
				break;
			case 13: // [AO2] ZONE_SILENT_WATCH_VOLUME
				strcpy(colname, "SilentVolLevel");
				break;
			case 14: // [AO2] ZONE_TROUBLE_TIMEOUT
				strcpy(colname, "TroubleTimeout");
				break;
			case 15: // [AO2] ZONE_CONFIG_TIME - Time since last zone configuration change
				strcpy(colname, "TIMESTAMPDIFF( MINUTE, Timestamp, NOW() )");
				break;
			case 16: // [AO2] ZONE_CURRENT_VOLUME
			case 17: // [AO2] WATCHMAN_HOUSE_VOLUME
				strcpy(colname, "CurrVolLevel");
				if ( address == 18 ) __client = "10.100.1.3";
				break;
			case 18: // [AO2] ZONE_IFACE_STREAM_NO
				strcpy(colname, "CurrVolLevel");
				break;
			case 19: // [AO2] ZONE_IFACE_STREAM_URL
				strcpy(colname, "CurrVolLevel");
				break;
			case 20: // [AO2] ZONE_IFACE_ALARM
				strcpy(colname, "CurrVolLevel");
				break;
			case 21: // [AO2] ZONE_IFACE_ERROR
				strcpy(colname, "CurrVolLevel");
				break;
		}

		if ( colname )
		{

			ResultSet_T r = Connection_executeQuery(
				con,
			    "SELECT IFNULL(%s, 0) AS Val "
			    "FROM AlertZones "
			    "WHERE HostAddr = '%s'",
			    colname,
			    __client
			);

			i = -1;
			if ( ResultSet_next(r) )
				i = ResultSet_getInt(r, 1);


		}
	}

    /*
     * Communication Errors
     */
	else if ( address == 22 || address == 23 || address == 24 )
	{
		ResultSet_T r = Connection_executeQuery(
			con,
			"SELECT ObjId, TIMESTAMPDIFF( MINUTE, Timestamp, NOW() ) AS Elapsed, Cleared "
			"FROM CommLog "
			"WHERE ActivateZone = 1 AND Ack = 0 "
			"ORDER BY ObjId DESC "
		);

    	if (DEBUG )
    		printf("  Comm Status Inqury (%d) \n", address);

		if ( ResultSet_next(r) )
		{
			// [AO21] WATCHMAN_ICAD_ACTIVE_ERROR
			if ( address == 22 ) i = ResultSet_getIntByName(r, "ObjId");

			// [AO22] WATCHMAN_ICAD_ERROR_ELAPSED
			else if ( address == 23 )
			{
				i = ResultSet_getIntByName(r, "Elapsed");
				if ( i > 1440 )
					i = 1440;
			}

			// [AO23] WATCHMAN_ICAD_ERROR_CLEARED
			else if ( address == 24 ) i = ResultSet_getIntByName(r, "Cleared");
		}
	}

    /*
     * Alert Group Membership
     */
	else if ( address == 25 )
	{
    	if (DEBUG )
    		printf("  Alert Group Membership (%d) \n", address);

		/*
		ResultSet_T r = Connection_executeQuery(
			con,
			"SELECT t1.GroupID "
			"FROM AlertGroups t2 "
		);

		if ( ResultSet_next(r) )
		{
			// [AO24] WATCHMAN_GROUP_MEMBERSHIP
			i = ResultSet_getIntByName(r, "GroupID");
		}
		*/
		i = 0;
	}

    if ( con )
    {
		Connection_clear(con);
		Connection_close(con);
    }

    if(DEBUG)
    	printf("  Response: [%d]\n", i);

    return i;
}

void modbus_set_coil_status(void *data, uint16_t address, uint8_t value, char *__ipstr)
{
	if ( DEBUG )
		printf("Client: [%s] Set coil status [%d=>%d]\n", __ipstr, address, value);

	Connection_T con = ConnectionPool_getConnection(pool);

    if ( address == 0 )
    {

    }


    if ( con )
    {
		Connection_clear(con);
		Connection_close(con);
    }
}

void modbus_set_holding_register(void *data, uint16_t address, uint16_t value, char *__ipstr)
{
	if ( DEBUG )
		printf("Client: [%s] Set holding register [%d=>%d]\n", __ipstr, address, value);

	Connection_T con = ConnectionPool_getConnection(pool);

	/*
	 * Function: 		Set zone volume
	 * Address Range: 	1-3
	 * 		Input 1:	Zone Volume Level (int 0-100)
	 * 		Input 2:	Silent Watch Active (bool)
	 * 		Input 3:	Silent Volume Level (int 0-100)
	 */
    if ( address == 1 || address == 2 || address == 3 )
    {
		ResultSet_T r = Connection_executeQuery(
			con,
			"SELECT iFaceHostAddr, iFaceTcpControlPort, SilentWatch "
			"FROM AlertZones "
			"WHERE HostAddr = '%s' ",
			__ipstr
		);

		if ( ResultSet_next(r) )
		{
			const char *iface_addr = ResultSet_getStringByName(r, "iFaceHostAddr");
			int port = ResultSet_getIntByName(r, "iFaceTcpControlPort");
			int swflag = ResultSet_getIntByName(r, "SilentWatch");

			if ( address == 1 )
			{
				char *cmd;
				char *resp;

				sprintf(cmd, "V=%d", value);

				printf("Setting vol [%s] on iFace [%s]\n", cmd, iface_addr);

				ResultSet_T r = Connection_executeQuery(
					con,
					"UPDATE AlertZones "
					"SET CurrVolLevel = '%d' "
					"WHERE HostAddr = '%s'",
					value,
					__ipstr
				);

				writeTcpCmd(iface_addr, cmd);
			}
			else if ( address == 3 )
			{
				ResultSet_T r = Connection_executeQuery(
					con,
					"UPDATE AlertZones "
					"SET SilentVolLevel = '%d' "
					"WHERE HostAddr = '%s'",
					value,
					__ipstr
				);
			}
		}
    }
    /*
     * Function:		Alert Zone Reset
     * Address Range:	4-6
     * 		Input 4:	Silent Watch Flag (bool)
     * 		Input 5:	Silent Volume Level (int 0-100)
     * 		Input 6:	Default Volume Level (int 0-100)
     * 		Input 7:	Zone reset init
     */
    else if ( address == 4 || address == 5 || address == 6 || address == 7 )
    {
    	printf("[%d] Alert Zone Reset Request: [%d]\n", address, value);

    	if ( address == 4 ) // Update silent watch flag
    	{
			ResultSet_T r = Connection_executeQuery(
				con,
				"UPDATE AlertZones "
				"SET SilentWatch = '%d' "
				"WHERE HostAddr = '%s'",
				value,
				__ipstr
			);

    	}
    	else if ( address == 5 ) // Update silent watch volume level
    	{
			ResultSet_T r = Connection_executeQuery(
				con,
				"UPDATE AlertZones "
				"SET SilentVolLevel = '%d' "
				"WHERE HostAddr = '%s'",
				value,
				__ipstr
			);
    	}
    	else if ( address == 6 ) // Update default volume level
    	{
			ResultSet_T r = Connection_executeQuery(
				con,
				"UPDATE AlertZones "
				"SET DefaultVolLevel = '%d' "
				"WHERE HostAddr = '%s'",
				value,
				__ipstr
			);
    	}
    	else
    	{

			ResultSet_T r = Connection_executeQuery(
				con,
				"SELECT IFNULL(iFaceHostAddr, 0), IFNULL(iFaceTcpControlPort, 0), IFNULL(DefaultVolLevel, 0), IFNULL(SilentWatch, 0), IFNULL(SilentVolLevel, 0), IFNULL(iFaceSerialPort, 0), IFNULL(IoResetAddr, 0) "
				"FROM AlertZones "
				"WHERE HostAddr = '%s' ",
				__ipstr
			);

			if ( ResultSet_next(r) )
			{
				char cmd[10];

				const char *iface_ip = ResultSet_getStringByName(r, "iFaceHostAddr");
				int tcp_port = ResultSet_getIntByName(r, "iFaceTcpControlPort");
				int defvol =  ResultSet_getIntByName(r, "DefaultVolLevel");
				int silent_active = ResultSet_getIntByName(r, "SilentWatch");
				int silvol = ResultSet_getIntByName(r, "SilentVolLevel");
				int serialport = ResultSet_getIntByName(r, "iFaceSerialPort");
				int reset_chan = ResultSet_getIntByName(r, "IoResetAddr");

				if ( silent_active == 1 )
					sprintf(cmd, "V=%d", silvol);
				else
					sprintf(cmd, "V=%d", defvol);

				printf("Reseting Zone Volume [%s] on iFace [%s] \n", cmd, iface_ip);

				writeTcpCmd(iface_ip, cmd);

				if ( value != 2 && serialport && reset_chan >= 0 )
				{
					char exec_cmd[128];

					sprintf(exec_cmd, "/usr/local/watchman-alerting/bin/modbus-write -t 3 -h %s -p %d -a '%d=1'", iface_ip, serialport, reset_chan);
					system( exec_cmd );

					sleep(3);

					sprintf(exec_cmd, "/usr/local/watchman-alerting/bin/modbus-write -t 3 -h %s -p %d -a '%d=0'", iface_ip, serialport, reset_chan);
					system( exec_cmd );
				}
			}
    	}
    }
    /*
     * Function:		Set House Volume Level
     * Address Range:	14
     * 		Input 14:	House Vol Level (int 0-100)
     */
    else if ( address == 14 )
    {
		ResultSet_T r = Connection_executeQuery(
			con,
			"SELECT iFaceHostAddr, iFaceTcpControlPort "
			"FROM AlertZones "
			"WHERE ZoneID = 'DEFAULT-1' "
		);

		char cmd[10];

		if ( ResultSet_next(r) )
		{
			const char *iface_ip = ResultSet_getStringByName(r, "iFaceHostAddr");
			int port = ResultSet_getIntByName(r, "iFaceTcpControlPort");

			sprintf(cmd, "V=%d", value);

			printf("Setting vol [%s] on House iFace \n", cmd);
			writeTcpCmd(iface_ip, cmd);
		}
    }
    /*
     * Function:		Comm Error Acknoledgement
     * Address Range:	16
     * 		Input 16:	Comm ID
     */
    else if ( address == 16 )
    {
		ResultSet_T r = Connection_executeQuery(
			con,
			"UPDATE CommLog "
			"SET Ack = '1' "
			"WHERE ObjId = '%d' ",
			value
		);
    }
    else
    {
		char colname[64];

		switch (address)
		{
			case 3:
				strcpy(colname, "iFaceHostAddr");
				break;
			case 4:
				strcpy(colname, "iFaceTcpControlPort");
				break;
			case 5:
				strcpy(colname, "iFaceSerialPort");
				break;
			case 6:
				strcpy(colname, "IoResetAddr");
				break;
			case 7:
				strcpy(colname, "DefaultVolLevel");
				break;
			case 8:
				strcpy(colname, "AlertVolLevel");
				break;
			case 9:
				strcpy(colname, "AutoUpdate");
				break;
			case 10:
				strcpy(colname, "SilentWatch");
				break;
			case 11:
				strcpy(colname, "SilentStartTime");
				break;
			case 12:
				strcpy(colname, "SilentEndTime");
				break;
			case 13:
				strcpy(colname, "SilentVolLevel");
				break;
		}

		if ( colname )
		{
			printf("Update %s => %d\n", colname, value);
			ResultSet_T r = Connection_executeQuery(
				con,
				"UPDATE AlertZones "
				"SET %s = '%d' "
				"WHERE HostAddr = '%s'",
				colname,
				value,
				__ipstr
			);
		}
    }

    if ( con )
    {
		Connection_clear(con);
		Connection_close(con);
    }

    return;
}

void int2Ipaddr(int ip)
{
    unsigned char bytes[4];
    bytes[0] = ip & 0xFF;
    bytes[1] = (ip >> 8) & 0xFF;
    bytes[2] = (ip >> 16) & 0xFF;
    bytes[3] = (ip >> 24) & 0xFF;
    printf("%d.%d.%d.%d\n", bytes[3], bytes[2], bytes[1], bytes[0]);
}

int ipToInt(int first, int second, int third, int fourth)
{
    return (first << 24) | (second << 16) | (third << 8) | (fourth);
}

int writeTcpCmd(char *host, char *cmd)
{
	int sd, rc, length = sizeof(int);
	struct sockaddr_in serveraddr;
	char buffer[BUFF_LEN];
	char server[255];
	char temp;
	int totalcnt = 0;
	struct hostent *hostp;
	char data[3];

	if ( ( sd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
	{
		perror("Client-socket() error");
		return -1;
	}

	if ( host )
		strcpy(server, host);
	else
	{
		perror("Missing host address \n");
		return -1;
	}

	if ( cmd )
		strcpy(data, cmd);
	else
	{
		perror("Missing text command\n");
		return -1;
	}

	memset(&serveraddr, 0x00, sizeof(struct sockaddr_in));
	serveraddr.sin_family = AF_INET;
	serveraddr.sin_port = htons(REMOTE_PORT);

	if((rc = connect(sd, (struct sockaddr *)&serveraddr, sizeof(serveraddr))) < 0)
	{
		perror("Client-connect() error");
		close(sd);
		return -1;
	}

	rc = write(sd, data, sizeof(data));

	if(rc < 0)
	{
		perror("Client-write() error");
		rc = getsockopt(sd, SOL_SOCKET, SO_ERROR, &temp, &length);

		if(rc == 0)
		{
			errno = temp;
			perror("SO_ERROR was");
		}
		close(sd);
		return -1;
	}

	totalcnt = 0;

	rc = recv(sd, buffer, sizeof(buffer) - 1, 0);
	if ( rc < 0 )
	{
		perror("Client-read() error");
		close(sd);
		return -1;
	}
	else if (rc == 0)
	{
		printf("Server program has issued a close()\n");
		close(sd);
		return -1;
	}

	printf("Client-read() is OK\n");
	printf("Response (%d) %s \n", rc, buffer);

	close(sd);

	return 0;
}

int main(int argc, char *argv[])
{
    int master_socket;
    int rc;
    fd_set refset;
    fd_set rdset;

    char uri[200];
    char tmp[200];
    char *pwd, *params, *user;
    char Database[] = "WatchmanAlerting";
    char Host[] = "localhost";
    char port[] = "3306";

    uri[0] = '\0';
    tmp[0] = '\0';
    tmp[0] = '\0';

    if ( argc > 1 )
    {

		int i;
		for ( i = 1; i < argc; i++ )
		{
			if ( strcmp(argv[i], "-h") == 0)
				snprintf(Host, 100, "%s", argv[++i]);
			else if ( strcmp(argv[i], "-P") == 0)
				snprintf(port, 20, "%s", argv[++i]);
			else if ( strcmp(argv[i], "-S") == 0)
				snprintf(tmp, 100, "%sunix-socket=%s&", tmp, argv[++i]);
			else if ( strcmp(argv[i], "-u") == 0)
				user = argv[++i];
			else if ( strcmp(argv[i], "-p") == 0)
				pwd = argv[++i];
			else if ( strcmp(argv[i], "-D") == 0)
				snprintf(Database, 100, "%s", argv[++i]);
			else if ( strcmp(argv[i], "-DEBUG") == 0)
				DEBUG = 1;

			else if ( i > 0 && argv[i][0] == 45 )
			{
				printf("Unknown Option: %s \n", argv[i]);
				fprintf(stderr, "Usage: %s [-h ipaddr] [-S unix-sock] [-u user] [-p password] [-D Database]\n\n", argv[0]);
				exit(1);
			}
		}
    }

    snprintf(uri, 200, "mysql://%s:%s/%s?user=%s&password=%s&%s", Host, port, Database, user, pwd, tmp);

    if ( DEBUG)
    	printf("Debug flag is set\n");

    printf("Opening database connection to URI [%s]\n", uri);

    URL_T url = URL_new(uri);
	pool = ConnectionPool_new(url);
	ConnectionPool_setReaper(pool, 360);
	ConnectionPool_start(pool);

    /* Maximum file descriptor number */
    int fdmax, recv_data;
    char recv_buffer[80];

    ctx = modbus_new_tcp("0.0.0.0", 1502, DEBUG);
    //ctx2 = modbus_new_tcp("0.0.0.0", 1503, DEBUG);

    mb_mapping = modbus_mapping_new(
        UT_BITS_ADDRESS + UT_BITS_NB,
        UT_INPUT_BITS_ADDRESS + UT_INPUT_BITS_NB,
        UT_REGISTERS_ADDRESS + UT_REGISTERS_NB,
        UT_INPUT_REGISTERS_ADDRESS + UT_INPUT_REGISTERS_NB);

    if (mb_mapping == NULL) {
        fprintf(stderr, "Failed to allocate the mapping: %s\n",
                modbus_strerror(errno));
        modbus_free(ctx);
        //modbus_free(ctx2);
        return -1;
    }

    mb_mapping->get_coil_status = &modbus_get_coil_status;
    mb_mapping->get_input_status = &modbus_get_input_status;
    mb_mapping->get_input_register = &modbus_get_input_register;
    mb_mapping->get_holding_register = &modbus_get_holding_register;
    mb_mapping->set_coil_status = &modbus_set_coil_status;
    mb_mapping->set_holding_register = &modbus_set_holding_register;

    server_socket = modbus_tcp_listen(ctx, NB_CONNECTION);
    //ascii_socket = modbus_tcp_listen(ctx2, NB_CONNECTION);

    signal(SIGINT, close_sigint);

    /* Clear the reference set of socket */
    FD_ZERO(&refset);

    /* Add the server socket */
    FD_SET(server_socket, &refset);
    //FD_SET(ascii_socket, &refset);

    /* Keep track of the max file descriptor */
    fdmax = server_socket;


    //fdmax = max(server_socket, ascii_socket) + 1;
    for (;;) {
        rdset = refset;

        if (select(fdmax+1, &rdset, (fd_set *)0, (fd_set *)0, 0) == -1) {
        	perror("Select error\n");
        	exit;
        }

		for (master_socket = 0; master_socket <= fdmax; master_socket++) {
			if (FD_ISSET(master_socket, &rdset)) {

		        if (master_socket == server_socket)
				{
					/* A client is asking a new connection */
					socklen_t addrlen;
					struct sockaddr_in clientaddr;
					int newfd;

					/* Handle new connections */
					addrlen = sizeof(clientaddr);
					memset(&clientaddr, 0, sizeof(clientaddr));
					newfd = accept(server_socket, (struct sockaddr *)&clientaddr, &addrlen);
					if (newfd == -1) {
						perror("Server accept() error");
					} else {
						FD_SET(newfd, &rdset);

						if (newfd > fdmax) {
							/* Keep track of the maximum */
							fdmax = newfd;
						}
						printf("New connection from %s:%d on socket %d\n", inet_ntoa(clientaddr.sin_addr), clientaddr.sin_port, newfd);
					}
				}
				else
				{
					uint8_t query[MODBUS_TCP_MAX_ADU_LENGTH];
					socklen_t __len;
					struct sockaddr_storage __addr;
					char __ipstr[INET6_ADDRSTRLEN];

					__len = sizeof __addr;
					getpeername(master_socket, (struct sockaddr*)&__addr, &__len);
					struct sockaddr_in *s = (struct sockaddr_in *)&__addr;
					inet_ntop(AF_INET, &s->sin_addr, __ipstr, sizeof __ipstr);

					modbus_set_socket(ctx, master_socket);
					rc = modbus_receive(ctx, query);

					if ( DEBUG )
						printf("Incoming request on socket [%d]: [%s] Receive Result: [%d]\n", master_socket, (unsigned char *)query, rc);

					if (rc==-99)
					{
						if (DEBUG)
							printf("Processing INIT Request: [%s]\n", (unsigned char *)query);

						int len;
						char *resp_str;
						char send_data[80];
						char *__ip = malloc(sizeof(char)*INET6_ADDRSTRLEN);
						__ip = __ipstr;

						if (strcmp(substring(query, 4,2),"09") == 0)
						{
							int buffsize = (sizeof(uint8_t) * MODBUS_TCP_MAX_ADU_LENGTH) + (sizeof(char) * 10);
							resp_str = malloc(buffsize);

							len = snprintf(resp_str, buffsize, "REQUEST=%s", (char*)query);
						}
						else
							len = zone_init(&resp_str, 200, &__ip, substring(query, 4, 2));

						int s = send(master_socket, (const char *)resp_str, strlen(resp_str), 0);

						if (DEBUG)
							printf("INIT Response: [%s] (%d bytes) \n",  resp_str, s);

						if (DEBUG)
							printf("Closing INIT connection on socket %d\n\n", master_socket);

						close(master_socket);

						free(resp_str);
						free(__ip);

						FD_CLR(master_socket, &rdset);

						if (master_socket == fdmax) {
							fdmax--;
						}

					}
					else if (rc > 0)
					{
						if (DEBUG)
							printf("Processing MODBUS Request\n");

						modbus_reply(ctx, query, rc, mb_mapping, __ipstr);
					}
					else if (rc == -1)
					{
						if (DEBUG)
							printf("Closing MODBUS connection on socket %d\n\n", master_socket);

						close(master_socket);

						/* Remove from reference set */
						FD_CLR(master_socket, &rdset);

						if (master_socket == fdmax) {
							fdmax--;
						}
					}
				}
			}
        }
    }

    return 0;
}
