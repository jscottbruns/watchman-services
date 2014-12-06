#ifndef ALERTZONE_SERVER_H_
#define ALERTZONE_SERVER_H_

/* Constants defined by configure.ac */
#define HAVE_INTTYPES_H 1
#define HAVE_STDINT_H 1

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif
#ifdef HAVE_STDINT_H
# ifndef _MSC_VER
# include <stdint.h>
# else
# include "stdint.h"
# endif
#endif

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

#define SERVER_ID         17
#define INVALID_SERVER_ID 18

const uint16_t UT_BITS_ADDRESS = 0x13;
const uint16_t UT_BITS_NB = 0x25;

const uint16_t UT_INPUT_BITS_ADDRESS = 0xC4;
const uint16_t UT_INPUT_BITS_NB = 0x16;

const uint16_t UT_REGISTERS_ADDRESS = 0x6B;
const uint16_t UT_REGISTERS_ADDRESS_SPECIAL = 0x6C;
const uint16_t UT_REGISTERS_ADDRESS_INVALID_TID_OR_SLAVE = 0x6D;
const uint16_t UT_REGISTERS_NB = 0x3;
/* If the following value is used, a bad response is sent.
   It's better to test with a lower value than
   UT_REGISTERS_NB_POINTS to try to raise a segfault. */
const uint16_t UT_REGISTERS_NB_SPECIAL = 0x2;

const uint16_t UT_INPUT_REGISTERS_ADDRESS = 0x08;
const uint16_t UT_INPUT_REGISTERS_NB = 0x1;

const float UT_REAL = 916.540649;
const uint32_t UT_IREAL = 0x4465229a;

uint8_t modbus_get_coil_status(void *data, uint16_t address, char *__ipstr);
void modbus_set_coil_status(void *data, uint16_t address, uint8_t value, char *__ipstr);
uint8_t modbus_get_input_status(void *data, uint16_t address, char *__ipstr);
uint16_t modbus_get_input_register(void *data, uint16_t address, char *__ipstr);
uint16_t modbus_get_holding_register(void *data, uint16_t address, char *__ipstr);
void modbus_set_holding_register(void *data, uint16_t address, uint16_t value, char *__ipstr);


#endif /* ALERTZONE_SERVER_H_ */
