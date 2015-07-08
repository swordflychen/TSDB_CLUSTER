#
# date: 2014-08-06
#

TARGET=tsdb
OBJ_DIR=./obj
SRC=./src
INC_DIR=-I./lib/libev-4.15 \
	-I./lib/json-c \
	-I./lib/leveldb-1.14.0/include \
	-I./lib/zookeeper-3.4.6/src/c/include \
	-I./lib/zookeeper-3.4.6/src/c/generated

LIB_DIR=./lib

LIBS=-lpthread -lstdc++ -lm -lrt
LIBA=-lev -ljson-c -lleveldb -lsnappy -lzookeeper_mt

CFLAGS=-Wall -g -DOPEN_PTHREAD -DOPEN_STATIC -DOPEN_COMPRESSION -DOPEN_DEBUG
#CFLAGS=-Wall -O2 -DOPEN_PTHREAD -DOPEN_STATIC -DOPEN_COMPRESSION

OBJ = $(addprefix $(OBJ_DIR)/, \
      ldb.o \
      main.o \
      read_cfg.o \
      ctl.o \
      utils.o \
      zk.o \
      logger.o \
      binlog.o \
      slave.o \
      )


help:
	@echo "make libs first!"

libs:
	$(MAKE) -C $(LIB_DIR) clean
	$(MAKE) -C $(LIB_DIR)

all:$(OBJ)
	gcc $(CFLAGS) $^ $(INC_DIR) -L$(LIB_DIR) $(LIBS) $(LIBA) -o $(TARGET)

$(OBJ_DIR)/%.o:$(SRC)/%.c
	@-if [ ! -d $(OBJ_DIR) ];then mkdir $(OBJ_DIR); fi
	gcc $(CFLAGS) $(INC_DIR) -L$(LIB_DIR) $(LIBS) -c $< -o $@

mc:
	nohup valgrind --tool=memcheck  --leak-check=full --verbose ./$(TARGET)&

push:
	git push -u origin master

clean:
	rm -f $(TARGET)
	rm -rf $(OBJ_DIR)
	@-if [ ! -d $(OBJ_DIR) ];then mkdir $(OBJ_DIR); fi

distclean:
	$(MAKE) clean
	$(MAKE) -C $(LIB_DIR) clean

install:
	mkdir -p /data/tsdb
	cp ./tsdb /data/tsdb
	cp ./config.json /data/tsdb
	cp ./tsdb_start.sh /data/tsdb
	cp ./tsdb_stop.sh /data/tsdb

