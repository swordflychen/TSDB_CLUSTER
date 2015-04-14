SRC_DIR=./src
OBJ_DIR=./obj
OBJ = $(addprefix $(OBJ_DIR)/, zk.o luazk.o) 
CFLAGS = -shared -fPIC -g
INCS = -I/usr/local/include/luajit-2.0/ \
       -I/usr/include/ \
       -I../../../lib/zookeeper-3.4.6/src/c/include \
       -I../../../lib/zookeeper-3.4.6/src/c/generated
LNKS = -L../../../lib/
LIBS = -lluajit-5.1 -lm -pthread -lzookeeper_mt


all: prepare libluazk.so c_test

prepare:
	-@if [ ! -d $(OBJ_DIR) ]; then mkdir $(OBJ_DIR); fi

libluazk.so: $(OBJ)
	gcc $^ $(CFLAGS) $(INCS) $(LNKS) $(LIBS) -o $@

c_test: ./obj/zk.o ./obj/test.o
	gcc $^ -fPIC -g $(INCS) $(LNKS) $(LIBS) -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	gcc $(CFLAGS) $(INCS) -c $< -o $@

clean:
	rm -rf $(OBJ_DIR) *.so *.a c_test

.PHONY: all prepare clean

