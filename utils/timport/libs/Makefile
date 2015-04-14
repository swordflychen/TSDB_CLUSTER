#
#
#


OBJ = libhiredis.a

all:$(OBJ)
	-@echo "Done LIB!"

libhiredis.a:
	-rm -rf $@
	$(MAKE) -C ./hiredis
	cp ./hiredis/$@ ./

clean:
	rm -f *.a *.so

distclean: clean
	$(MAKE) -C ./hiredis clean

.PHONY:
	all clean distclean
