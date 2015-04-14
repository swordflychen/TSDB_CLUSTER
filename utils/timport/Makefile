#
# 2014-11-10
#

TARGET=timport
OBJ_DIR=./obj
SRC_DIR=./src
LIB_DIR=./libs

LNK_LIB=$(LIB_DIR)/libhiredis.a
INC_DIR=-I$(LIB_DIR)/hiredis
#CFLAGS=-g -Wall -Wpadded
CFLAGS=-g -Wall

OBJ=$(addprefix $(OBJ_DIR)/, \
    main.o \
    logger.o \
    backup.o \
    timport.o \
    common.o \
    db.o \
    statistics.o \
    )

help:
	@echo "make libs first."

libs:
	$(MAKE) -C $(LIB_DIR)

all: $(OBJ)
	gcc $(CFLAGS) -o $(TARGET) $^ $(LNK_LIB)

clean:	
	rm -rf $(OBJ_DIR) $(TARGET) nohup.out

distclean: clean
	$(MAKE) -C $(LIB_DIR) distclean

$(OBJ_DIR)/%.o:$(SRC_DIR)/%.c
	@-if [ ! -d $(OBJ_DIR) ];then mkdir $(OBJ_DIR); fi 
	gcc $(CFLAGS) $(INC_DIR) -c $< -o $@

mc:
	nohup valgrind --tool=memcheck  --leak-check=full --verbose ./$(TARGET)&

.PHONY: help libs all clean distclean


