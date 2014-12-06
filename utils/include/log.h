#ifndef LOG_H_
#define LOG_H_

#define E_WARN	0
#define E_DEBUG	1
#define E_ERROR	2
#define E_CRIT	3

#define LOG(msg, lvl, args...) __LOG((msg), __FILE__, __LINE__, (lvl), ## args)

extern int fp;

void __LOG( const char* message, const char* fname, int lineno, int level, ... );
int init_log(char * path);
int close_log(void);

#endif /* LOG_H_ */
