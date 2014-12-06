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
#include <signal.h>
#include <sys/time.h>
#include <stdlib.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <zdb.h>

#include "httpd.h"

URL_T url;
ConnectionPool_T pool;

char* substring(const char* str, size_t begin, size_t len)
{
  if (str == 0 || strlen(str) == 0 || strlen(str) < begin || strlen(str) < (begin+len))
    return 0;

  return strndup(str + begin, len);
}


void myError404Handler(server, error)
	httpd	*server;
	int	error;
{

}

void zone_init(server)
httpd *server;
{
	Connection_T con = ConnectionPool_getConnection(pool);
	httpVar *variable;

	if ( ! con )
	{
		printf("Error accessing database pool\n");
		return;
	}

	printf("** Zone configuration requested by client [%s]\n", server->clientAddr);

	variable = httpdGetVariableByName(server, "action");
	if (variable == NULL)
	{
		httpdPrintf(server,"Not Found");
		Connection_clear(con);
		Connection_close(con);
		return;
	}

	printf("Action requested => [%s]\n", variable->value);

	if ( strcmp(variable->value, "INI") == 0 || strcmp(variable->value, "INIT01") == 0 || strcmp(variable->value, "INIT02") == 0 )
	{
		ResultSet_T r = Connection_executeQuery(
			con,
		    "SELECT ZoneID, ZoneName, iFaceHostAddr, DefaultVolLevel, AlertVolLevel, SilentWatch, SilentStartTime, SilentEndTime, SilentVolLevel "
		    "FROM AlertZones "
		    "WHERE HostAddr = '%s'",
		    server->clientAddr
		);

		if ( ResultSet_next(r) )
		{
			const char *ZoneID =  ResultSet_getStringByName(r, "ZoneID");
			const char *ZoneName =  ResultSet_getStringByName(r, "ZoneName");

			if ( strcmp(variable->value, "INI") == 0 )
			{
				printf("Returning [INI] configuration => ZID=%s&ZLO=%s\n", ZoneID, ZoneName);
				Connection_clear(con);
				Connection_close(con);
				httpdPrintf(server, "ZID=%s&ZLO=%s", ZoneID, ZoneName);
			}
			if ( strcmp(variable->value, "INIT01") == 0 )
			{
				printf("Returning [ZID] configuration => %s \n", ZoneID);

				Connection_clear(con);
				Connection_close(con);

				httpdPrintf(server, "%s", ZoneID);
				return;
			}
			if ( strcmp(variable->value, "INIT02") == 0 )
			{
				printf("Returning [ZLO] configuration => %s \n", ZoneName);
				Connection_clear(con);
				Connection_close(con);
				httpdPrintf(server, "%s", ZoneName);
			}
		}
	}
	else if ( strcmp(variable->value, "INIT03") == 0 )
	{
		ResultSet_T r = Connection_executeQuery(
			con,
		    "SELECT t2.GroupName "
		    "FROM AlertGroupMember t1 "
		    "LEFT JOIN AlertGroups t2 ON t2.GroupAddr = t1.GroupAddr "
		    "LEFT JOIN AlertZones t3 ON t3.ZoneID = t1.ZoneID "
		    "WHERE t3.HostAddr = '%s'",
		    server->clientAddr
		);

		char Units[150];

		while ( ResultSet_next(r) )
		{
			strcat(Units, (strlen(Units) > 0 ? "&": ""));
			strcat(Units, ResultSet_getStringByName(r, "GroupName"));

			printf("Returning [UME] configuration => %s \n", Units);
			httpdPrintf(server, "%s", Units);
			return;
		}

	    if ( con )
	    {
			Connection_clear(con);
			Connection_close(con);
	    }
	}

}

void device_monitor(server)
httpd *server;
{
	Connection_T con = ConnectionPool_getConnection(pool);
	if ( ! con )
	{
		printf("Can't get connection from db connection pool");
		return;
	}
	httpVar *variable;

	char *url, *mac_addr, *ip_addr, *error_no, *stream_no, *uptime, *curr_volume;
	int alarm;

	if ( ! con )
	{
		printf("Error accessing database pool\n");
		return;
	}

	variable = httpdGetVariableByName(server, "mac");
	if (variable != NULL)
		mac_addr = variable->value;

	variable = httpdGetVariableByName(server, "alarm");
	if (variable != NULL)
	{
		if ( strcmp(variable->value, "true") == 0 )
			alarm = 1;
	}

	variable = httpdGetVariableByName(server, "StreamNumber");
	if (variable != NULL)
		stream_no = variable->value;

	variable = httpdGetVariableByName(server, "Error");
	if (variable != NULL)
		error_no = variable->value;

	variable = httpdGetVariableByName(server, "Volume");
	if (variable != NULL)
		curr_volume = variable->value;

	variable = httpdGetVariableByName(server, "UpTime");
	if (variable != NULL)
		uptime = variable->value;

	variable = httpdGetVariableByName(server, "URL");
	if (variable != NULL)
		url = variable->value;


	printf("MAC: [%s] ALARM: [%d] STREAM [%s] ERROR [%s] VOL [%s] UPTIME [%s] URL [%s] \n", mac_addr, alarm, stream_no, error_no, curr_volume, uptime, url);

	ResultSet_T r = Connection_executeQuery(
		con,
		"UPDATE AlertZones SET "
		"CurrVolLevel = '%s', "
		"CurrStreamNo = '%s', "
		"CurrUrl = '%s', "
		"iFaceAlarm = '%d', "
		"iFaceError = '%s', "
		"iFaceUptime = '%s' "
		"WHERE iFaceHostAddr = '%s'",
		curr_volume,
		stream_no,
		url,
		alarm,
		error_no,
		uptime,
		server->clientAddr
	);

    if ( con )
    {
		Connection_clear(con);
		Connection_close(con);
    }

    return;
}

int main(argc, argv)
	int	argc;
	char	*argv[];
{
	httpd	*server;
	char 	*host;
	int 	port, errFlag, result;
	extern char *optarg;
	extern int optind, opterr, optopt;
	int c;
	struct	timeval timeout;

	host = "0.0.0.0";//NULL;
	port = 80;
	errFlag = 0;
	while ( (c=getopt(argc,argv,"h:p:")) != -1 )
	{
		switch ( c )
		{
			case 'h':
				host=optarg;
				break;

			case 'p':
				port = atoi(optarg);
				break;

			default:
				errFlag++;
		}
	}
	if (errFlag)
	{
		fprintf(stderr,"device-server usage: [-h <host IP>] [ -p <port >]\n");
		exit(1);
	}

	URL_T url = URL_new("mysql://localhost:3306/WatchmanAlerting?unix-socket=/var/lib/mysql/mysql.sock&user=watchman&password=");
	if ( ! url )
	{
		printf("Error connecting to database\n");
		exit(1);
	}
	pool = ConnectionPool_new(url);
	if ( ! pool )
	{
		printf("Error initiating database connection pool");
	}
	ConnectionPool_setReaper(pool, 10);
	ConnectionPool_start(pool);

	/*
	** Ensure that PIPE signals are either handled or ignored.
	** If a client connection breaks while the server is using
	** it then the application will be sent a SIGPIPE.  If you
	** don't handle it then it'll terminate your application.
	*/
	signal(SIGPIPE, SIG_IGN);

	/*
	** Create a server and setup our logging
	*/
	server = httpdCreate(host,port);
	if (server == NULL)
	{
		perror("Can't create server");
		exit(1);
	}
	httpdSetAccessLog(server, stdout);
	httpdSetErrorLog(server, stdout);

	/*
	** We are fussy and don't want the default Error 404 page
	*/
//	httpdSetErrorFunction(server,404, myError404Handler);

	/*
	** Setup some content for the server
	*/
	httpdAddCWildcardContent(server,"/sensors/data", NULL, device_monitor);
	httpdAddCContent(server,"/", "zone_init", HTTP_TRUE, NULL, zone_init);
	/*
	** Go into our service loop
	*/

	printf("Zone server listening on %s:%d\n", host, port);

	while(1 == 1)
	{
		/*
		** Linux modifies the timouet value during the
		** select call so we must set it everyt ime.  Most
		** other UNIX implementations do not modify timeout
		** but it doesn't hurt to set it each time anyway
		*/
		timeout.tv_sec = 5;
		timeout.tv_usec = 0;
		result = httpdGetConnection(server, &timeout);
		if (result == 0)
			continue;

		if (result < 0)
			continue;

		if(httpdReadRequest(server) < 0)
		{
			httpdEndRequest(server);
			continue;
		}
		httpdProcessRequest(server);
		httpdEndRequest(server);
	}
}
