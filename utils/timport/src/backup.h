/*
 * backup.h
 *
 *  Created on: Nov 12, 2014
 *      Author: chenjf
 */

#pragma once

#include <stdint.h>
#include <limits.h>

/* file name pattern. */
#define FILE_NAME_PATTERN_LEN 4
#define KIX_FILE_NAME_PATTERN "KIX_"
#define VAL_FILE_NAME_PATTERN "VAL_"

/*
 * index.data format: file_head item0 item1 ...
 * item format: IDX_VAL_START:IDX_VAL_LEN:\n
 */
#define KIX_ITEM_LEN        33
#define KIX_VAL_START       20
#define KIX_VAL_LEN         10


typedef struct _data_file
{
    char        fname[PATH_MAX];
    int32_t     fd;
    int64_t     pos;
    char        *map_mem;
} data_file_t;

typedef struct _backup
{
    char        path[PATH_MAX];

    int32_t     cur_idx;
//    data_file_t idx_file;
    data_file_t kix_file;
    data_file_t val_file;
} backup_t;

/*
 * initial backup file.
 */
backup_t *backup_open(const char *path);

/*
 * close backup file.
 */
int32_t backup_close(backup_t *backup);

/*
 * append data.
 */
int32_t backup_set(backup_t *backup, const char *key, int32_t klen, const char *val, int32_t vlen);

/*
 * get data random.
 */
int32_t backup_get(backup_t *backup, int cursor, char *key, int32_t *klen, char *val, int32_t *vlen);

/*
 * get data in order.
 */
int32_t backup_pop(backup_t *backup, char *key, int32_t *klen, char *val, int32_t *vlen);

