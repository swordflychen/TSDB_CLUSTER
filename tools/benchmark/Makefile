# target
TARGET = bench_test

# hiredis
HIREDIS_DIR = ../api/hiredis
HIREDIS = libhiredis.a

# object and source
OBJ_DIR = ./obj
SRC_DIR = ./src
OBJ = $(addprefix $(OBJ_DIR)/, \
      adlist.o \
      ae.o \
      anet.o \
      sds.o \
      zmalloc.o\
      redis-benchmark.o)

all: $(OBJ) $(HIREDIS_DIR)/$(HIREDIS)
	gcc -Wall -o $(TARGET) $^

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@-if [ ! -d $(OBJ_DIR) ]; then mkdir $(OBJ_DIR); fi
	gcc -Wall -I$(HIREDIS_DIR) -c $< -o $@

$(HIREDIS_DIR)/$(HIREDIS):
	$(MAKE) -C $(HIREDIS_DIR)

clean:
	rm -f $(OBJ) $(TARGET)

.PHONY: all clean
