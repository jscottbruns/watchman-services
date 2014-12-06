#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include "common.h"
#include "log.h"

int fp;

void __LOG( const char* message, const char* fname, int lineno, int level, ... )
{
	va_list arg;
	pid_t pid = getpid();
	time_t now = time(&now);
	char* _time = ctime(&now);
	char lvl[8], _fname[20], fmsg[500], buff[600];

	sprintf(_fname, "%s", fname);

	va_start(arg, level);
	vsprintf(fmsg, message, arg);
	va_end(arg);

	if (level == E_WARN) sprintf(lvl, "%s", "E_WARN");
	if (level == E_DEBUG) sprintf(lvl, "%s", "E_DEBUG");
	if (level == E_ERROR) sprintf(lvl, "%s", "E_ERROR");
	if (level == E_CRIT) sprintf(lvl, "%s", "E_CRIT");

	if ( fp )
	{
		sprintf(buff, "%s %s [%s:%d] %s (%d)\n", strtok(_time, "\n"), lvl, _fname, lineno, fmsg, pid);
		write(fp, buff, strlen(buff));
	}
	else
	{
		fprintf(stderr, "%s %s [%s:%d] %s (%d)\n", strtok(_time, "\n"), lvl, _fname, lineno, fmsg, pid);
	}
}

int init_log(char * path)
{
	if ( ( fp = open(path, O_WRONLY|O_APPEND, 0755) ) )
		return 0;

	return 1;
}

int close_log(void)
{
	return close(fp);
}
