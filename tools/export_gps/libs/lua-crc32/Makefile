# makefile for crclib
# based on bitlib's makefile

CFLAGS += -mtune=core2 -fPIC -W -Wall -O2 -I/usr/local/include/luajit-2.0 -L/usr/local/lib

OBJS = lcrc.o crc.o
SRCS = lcrc.c crc.c
AR = ar rcu
EXT = so 

T = crc.a

all: $T

# shared libraries (for Linux)

shared: $T
	$(CC) $(CFLAGS) -o crc.$(EXT) -shared *.o -lluajit-5.1
	strip crc.$(EXT)
$T: $(OBJS)
	$(AR) $@ $(OBJS)

clean:
	rm -f $(OBJS) $T *.$(EXT)
