#SHELL = /bin/sh
LIBS=-L/usr/lib/mysql ../../ICPDAS/LinPAC-8X81_sdk/lib/libi8k.a /usr/local/lib/libzdb.a -lpthread -lcrypt -lz -lm -ldl -lmysqlclient -lmysqlclient_r -lrt -lpopt /usr/local/lib/librabbitmq.a 
CFLAGS=-I. -I./include -I../../ICPDAS/LinPAC-8X81_sdk/include -I/usr/local/include/zdb -I/usr/local/include -I../utils/include

# add $(LinPAC) value for x86 LinPAC(LX800 and Atom)
PROCESSOR=$(shell uname -p)
CPU_TYPE=i$(shell echo $(PROCESSOR) | sed -ne 's/^i[3456].*/X/p')86

ifeq ($(CPU_TYPE), iX86)
        LinPAC = x86
endif

ifeq ($(LinPAC), x86)
        CROSS_COMPILE=
else
        CROSS_COMPILE=arm-linux-
endif

AS=$(CROSS_COMPILE)as
LD=$(CROSS_COMPILE)ld
CC=$(CROSS_COMPILE)gcc
CPP=$(CC) -E
AR=$(CROSS_COMPILE)ar
NM=$(CROSS_COMPILE)nm
STRIP=$(CROSS_COMPILE)strip
OBJCOPY=$(CROSS_COMPILE)objcopy
OBJDUMP=$(CROSS_COMPILE)objdump
DEBUGGER=gdb

ifdef DEBUG
CFLAGS+=-g -D DEBUG
endif

all:     gpio-server

gpio-server: clean
	$(CC) $(DFLAGS) -o gpio-server ./src/gpio-server.c ../utils/ini.c ../utils/log.c ../utils/utils.c $(CFLAGS) $(LIBS)

clean:
	rm -f ./src/*.o ../utils/*.o ./*~ ./*.tmp ./.bak ./gpio-server	
