/*
 * export.h
 *
 *  Created on: Nov 12, 2014
 *      Author: chenjf
 */

#pragma once

#include <stdint.h>
#include <time.h>

#include "hiredis.h"
#include "logger.h"
#include "common.h"
#include "backup.h"
#include "db.h"
#include "statistics.h"


typedef struct _timport
{
    /* backup. */
    backup_t *backup;

    /* database. */
    db_t *db;

    /* statistics. */
    statistics_t *stats;

    /* for urllog. */
    char kbuf[KEY_BUF_SIZE];
    char vbuf[VAL_BUF_SIZE];
    int32_t kbuf_len;
    int32_t vbuf_len;

    /* for others. */
    // ...

} timport_t;


int32_t timport_open(const char *stats_path, const char *backup_path, const database_t *redis, int32_t redis_size, const database_t *tsdb, const int16_t tsdb_size, const database_t *stats);

int32_t timport_close(void);

int32_t timport_urllog(int64_t time, int64_t time_limit);

int32_t timport_exurllog(int64_t ts, int64_t tl);

