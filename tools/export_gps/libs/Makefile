#
# date: 2014/12/22 #
#

LIBS=libluadecoder.so \
     crc.so


all: $(LIBS)

libluadecoder.so:
	$(MAKE) -C ./decoder
	cp ./decoder/$@ .

crc.so:
	$(MAKE) -C ./lua-crc32 shared
	cp ./lua-crc32/$@ .

clean:
	rm -f *.so

distclean: clean
	$(MAKE) -C ./decoder clean
	$(MAKE) -C ./lua-crc32 clean

.PHONY: all clean distclean
