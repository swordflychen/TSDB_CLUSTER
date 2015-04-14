/*
 * file: decode.h
 * auth: chenjianfei@daoke.me
 * date: 2014
 * desc: decoder.
 */


/* url item. */
typedef struct {
    char* data;
    int   len;
} Property;

/* decoder. */
int decode_impl(const char * src, Property** ppro, int* x, int* y);


