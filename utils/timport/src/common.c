/*
 * common.c
 *
 *  Created on: Nov 17, 2014
 *      Author: chenjf
 */

#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>

#include "common.h"


/* common function. */
inline int32_t get_column_count(const char *row, char sep)
{
    int32_t cols = 0;
    if (row == NULL || *row == sep) {
        return cols;
    }

    while (row != NULL && *row != 0) {
        ++ cols;
        ++ row;
        row = strchr(row, sep);
    }

    return cols;
}

/* get file size. */
inline int64_t get_file_size(const char *path)
{
    int64_t    file_size = -1;
    struct stat statbuff;

    if(stat(path, &statbuff) < 0) {
        return file_size;
    }
    else {
        file_size = statbuff.st_size;
        return file_size;
    }
}

/* open file. */


/* write data to file. */
inline int write_file(int32_t fd, const char *buf, int32_t count)
{
    int32_t write_size = 0;
    while (count > 0) {
        write_size = write(fd, buf, count);

        if (write_size < 0) {
            return IX_ERROR;
        }

        buf += write_size;
        count -= write_size;
    }

    return IX_OK;
}

inline char *x_strdup(const char *src)
{
    if (src == NULL)
        return NULL;

    int len = strlen(src);
    char *out = calloc(len + 1, sizeof(char));
    strcpy(out, src);
    return out;
}
