all:
	rm -rf modbus-server
	gcc  src/modbus-server.c -o modbus-server -I../libmodbus/src -I/usr/local/include/zdb -I/usr/include/mysql -I/usr/include -L/usr/lib -L/usr/lib/mysql ../libmodbus/src/.libs/libmodbus.a ../libzdb-2.11.1/.libs/libzdb.a -lmysqlclient -lmysqlclient_r -lpthread -lcrypt -lz -lm -ldl
