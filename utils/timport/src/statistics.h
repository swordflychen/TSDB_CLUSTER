/*
 * statistics.h
 *
 *  Created on: Nov 18, 2014
 *      Author: chenjf
 */

#pragma once

#include <stdint.h>
#include <limits.h>
#include "hiredis.h"
#include "common.h"


#define STAT_LEN            256

typedef struct _stats_url
{
    uint8_t     is_set;             /* record statistics or not. */
    int32_t     act_cnt;            /* active users count. */
    int32_t     url_cnt;            /* url log count. */
    int32_t     gps_cnt;            /* gps count. */

    int32_t     act_len;            /* the total length of active users.*/
    int32_t     url_len;            /* the total length of url logs. */
    int32_t     gps_len;            /* the total length of gps. */
} stats_url_t;

/* ... */

typedef struct _statistics
{
    redisContext *conn;

    char        fname[NAME_MAX];    /* file name. */
    int32_t     fd;                 /* file handle. */

    stats_url_t urllog;

    /* ... */
} statistics_t;

statistics_t *statistics_open(const char *path, const database_t *redis);

int32_t statistics_set_urllog(statistics_t *stats, const char *pattern, const char *tt);

int32_t statistics_close(statistics_t *stats);
