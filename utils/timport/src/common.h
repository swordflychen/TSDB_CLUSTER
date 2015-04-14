/*
 * common.h
 *
 *  Created on: Nov 12, 2014
 *      Author: chenjf
 */

#pragma once

#include <stdint.h>

/* used for configure. */
typedef struct _database
{
    char *ip;
    int32_t port;
} database_t;

/* error no. */
#define     IX_ERROR        -1
#define     IX_OK           0
#define     IX_NORESULT     1


/* buffer size: key:64 value: 2 * 1024 * 1024. */
#define KEY_BUF_SIZE 64
#define VAL_BUF_SIZE 2097152

/* common function. */
inline int32_t get_column_count(const char *row, char sep);

/* get file size. */
inline int64_t get_file_size(const char *path);

/* write data to file. */
inline int write_file(int32_t fd, const char *buf, int32_t count);

/* string copy. */
inline char *x_strdup(const char *src);
