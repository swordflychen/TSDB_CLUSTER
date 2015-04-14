/*
 * db.h
 *
 *  Created on: Nov 17, 2014
 *      Author: chenjf
 */

#pragma once

#include "hiredis.h"
#include "common.h"
#include "backup.h"
#include "statistics.h"

typedef struct _db
{
    int16_t         redis_size;
    int16_t         tsdb_size;

    redisContext    **redis_conns;     /* redis. */
    redisContext    **tsdb_conns;       /* tsdb. */
    redisReply      **reply;
} db_t;

db_t *db_open(const database_t *redis, int32_t redis_size, const database_t *tsdb, int32_t tsdb_size);

int32_t db_close(db_t *db);

redisReply *smembers_from_redis(redisContext *redis_conn, const char *kbuf, int32_t klen, uint64_t ts, uint16_t num);

int32_t smembers_from_redis_concat(redisContext *redis_conn, const char *kbuf, int32_t klen, char *vbuf, int32_t *vlen, uint64_t ts);

int32_t set_to_tsdb(redisContext *tsdb_conn, const char *kbuf, int32_t klen, const char *vbuf, int32_t vlen, uint64_t ts);

int32_t expire_from_redis(redisContext *redis_conn, const char *kbuf, int32_t klen, uint64_t tl);

int32_t handle_urlgps(redisContext *redis_conn, redisContext *tsdb_conn, backup_t *backup, const char *kbuf, int32_t klen, char *vbuf, int32_t *vlen, uint64_t ts, uint64_t tl);

int16_t get_tsdb_index(const char *imei, int32_t len);

int32_t handle_activeuser(db_t *db, backup_t *backup, const char *kbuf, int32_t klen, char *vbuf, int32_t *vlen, uint64_t ts, uint64_t tl);
