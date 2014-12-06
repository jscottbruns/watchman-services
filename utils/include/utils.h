/* vim:set ft=c ts=2 sw=2 sts=2 et cindent: */
#ifndef librabbitmq_examples_utils_h
#define librabbitmq_examples_utils_h

#define AMQP_LOG(x, context) amqp_exception((x), (context), __FILE__, __LINE__)

void die(const char *fmt, ...);
extern void die_on_error(int x, char const *context, const char* file, int lineno);
extern int amqp_exception(amqp_rpc_reply_t x, char const *context, const char* file, int lineno);

extern void amqp_dump(void const *buffer, size_t len);

extern uint64_t now_microseconds(void);
extern void microsleep(int usec);

extern int _rand (unsigned int min, unsigned int max);

#endif
