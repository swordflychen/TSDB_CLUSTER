
OBJ = luazk.o 
CFLAGS = -shared -fPIC
INCS = -I/usr/local/include/luajit-2.0/ \
       -I/usr/include/ \
       -I../../../lib/zookeeper-3.4.6/src/c/include \
       -I../../../lib/zookeeper-3.4.6/src/c/generated
LNKS = -L../../../lib/
LIBS = -lluajit-5.1 -lpthread -lzookeeper_mt


all: libluazk.so

libluazk.so: $(OBJ)
	gcc $^ $(CFLAGS) $(INCS) $(LNKS) $(LIBS) -o $@

%.o : %.c
	gcc $(CFLAGS) $(INCS) -c $< -o $@

clean:
	rm -f *.o *.so *.a

