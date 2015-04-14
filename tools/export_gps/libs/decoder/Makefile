all: prepare libluadecoder.so

OBJ_DIR=./obj
SRC_DIR=./src


libluadecoder.so: $(OBJ_DIR)/decode_impl.o $(OBJ_DIR)/lua_decoder.o 
	gcc -shared -fPIC -lluajit-5.1 -o $@ $^

$(OBJ_DIR)/lua_decoder.o: $(SRC_DIR)/lua_decoder.c
	gcc -fPIC -I/usr/local/include/luajit-2.0/ -I$(SRC_DIR) -c $< -o $@

$(OBJ_DIR)/decode_impl.o: $(SRC_DIR)/decode_impl.c
	gcc -fPIC -c $< -o $@

clean:
	rm -rf $(OBJ_DIR) *.so

prepare:
	@-if [ ! -d $(OBJ_DIR) ];then mkdir $(OBJ_DIR); fi

.PHONY: all clean prepare
