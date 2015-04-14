#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <sys/time.h>

#include "decode_impl.h"

/* parse header. */
static int parse_header(const char* src, int* x, int* y)
{
    char str_xy[2][10] = {0};
    char* pos = (char *)src;
    int len = strchr(src, '*') - src;
    memcpy(str_xy[0], pos, len);

    pos += len + 1;
    len = strchr(src, '@') - src - len - 1;
    memcpy(str_xy[1], pos, len);
    
    *x = (int)strtol(str_xy[0], NULL, 0);
    *y = (int)strtol(str_xy[1], NULL, 0);

    if( *x <= 0 || *y <= 0) {
        return 1;
    }

    return 0;
}

/* decode. */
int decode_impl(const char* src, Property** ppro, int* x, int* y)
{
    /* parse header. */
    if(parse_header(src, x, y) != 0){
        return 1;
    }

    /* alloc space. */
    Property* pro = (Property*) malloc( sizeof(Property) * (*x) * (*y)); 
    *ppro = pro;

    /* parse body. */
    int i;
    char * ptr = NULL;
    char *tmp = strchr(src, '@') + 1;
    for(i=0; i < (*x) * (*y); ++i) {
            pro->data = tmp;
            ptr = strchr(tmp, '|');
            if (ptr == 0) {
                pro->len = strlen(tmp); 
            } else {
                pro->len = ptr - tmp; 
            }   
            tmp = tmp + pro->len + 1;
            ++ pro;
    }
    
    /* deal result. */
    return 0;
}

// /* get the specified file size. */
// static int get_file_size(char* file)
// {
//     struct stat st; 
//     if (stat(file, &st) == 0) {
//         return st.st_size;
//     }   
//     return -1; 
// }
// 
// static int bak_main()
// {
//     /* get file size. */
//     char* filename = "./urlLog";
//     int filelen = get_file_size(filename);
// 
//     /* open file. */
//     int fd = open(filename, O_RDONLY);
//     if(fd < 0) {
//         perror("open file error");
//         return 1;
//     }
// 
//     /* allocate space and read file. */
//     char* buffer = (char*) malloc(filelen + 1);
//     memset(buffer, 0, filelen);
//     int ret = 0;
//     char* tmp = buffer;
//     int len = filelen;
//     while(len > 0) {
//         ret = read(fd, tmp, filelen);
//         if(ret < 0) {
//             perror("read file error");
//             free(buffer);
//             return 1;
//         }
//         len -= ret;
//         tmp += ret;
//     }
// 
//     /* close file. */
//     close(fd);
// 
//     /* decode. */
//     unsigned long costtime = 0;
//     struct timeval start, end;
//     Property* pro = NULL;
//     int x, y, i;
//     for(i=0; i<10000; ++i) {
//         gettimeofday(&start, NULL);
//         if(decode(buffer, &pro, &x, &y) != 0) {
//             perror("decode error. errno");
//             free(buffer);
//             return 1;
//         }
//         gettimeofday(&end, NULL);
//         costtime += (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec); 
//         
//         free(pro);
//         pro = NULL;
//     }
// 
//     printf("cost time: %d\n", costtime);
// 
//     /* show data. */
//     //int i;
//     //for(i=0; i<x*y; ++i) {
//     //    printf("len: %d, data: %s\n", pro[i].len, pro[i].data);
//     //    printf("\n");
//     //}
// 
//     /* free space. */
//     free(buffer);
// 
//     return 0;
// }
