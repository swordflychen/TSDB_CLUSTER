/*
 * db.c
 *
 *  Created on: Nov 17, 2014
 *      Author: chenjf
 */


#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "db.h"
#include "logger.h"

db_t *db_open(const database_t *redis, int32_t redis_size, const database_t *tsdb, int32_t tsdb_size)
{
    int32_t i;
    db_t *db = (db_t *)malloc(sizeof(db_t));
    db->redis_size = redis_size;
    db->redis_conns = (redisContext **) malloc(sizeof(redisContext *) * redis_size);

    db->tsdb_size = tsdb_size;
    db->tsdb_conns = (redisContext **) malloc(sizeof(redisContext *) * tsdb_size);

    db->reply = (redisReply **) malloc(sizeof(redisReply *) * redis_size);

    /* open redis. */
    for (i = 0; i < redis_size; ++i) {
        log_info("Connection to redis: ip[%s], port[%d]", redis[i].ip, redis[i].port);
        db->redis_conns[i] = redisConnect(redis[i].ip, redis[i].port);
        if (db->redis_conns[i] == NULL || db->redis_conns[i]->err) {
            if (db->redis_conns[i] != NULL) {
                log_error("Connection error: %s", db->redis_conns[i]->errstr);
            } else {
                log_error("Connection error: can't allocate tsdb context");
            }
            return NULL;
        }
    }

    /* open tsdb. */
    for (i = 0; i < tsdb_size; ++i) {
        log_info("Connection to tsdb: ip[%s], port[%d]", tsdb[i].ip, tsdb[i].port);
        db->tsdb_conns[i] = redisConnect(tsdb[i].ip, tsdb[i].port);
        if (db->tsdb_conns[i] == NULL || db->tsdb_conns[i]->err) {
            if (db->tsdb_conns[i] != NULL) {
                log_error("Connection error: %s", db->tsdb_conns[i]->errstr);
            } else {
                log_error("Connection error: can't allocate tsdb context");
            }
            return NULL;
        }
    }

    return db;
}

int32_t db_close(db_t *db)
{
    if (db != NULL) {
        int32_t i;
        /* close redis. */
        for (i = 0; i < db->redis_size; ++i) {
            redisFree(db->redis_conns[i]);
        }

        /* close tsdb. */
        for (i = 0; i < db->tsdb_size; ++i) {
            redisFree(db->tsdb_conns[i]);
        }

        /* free redis. */
        free(db->redis_conns);

        /* free tsdb. */
        free(db->tsdb_conns);

        /* close reply. */
        free(db->reply);

        free(db);
        db = NULL;
    }

    return IX_OK;
}


static int32_t sort_array(redisReply *reply)
{
    int32_t i, j, ltmp;
    char *tmp;

    /* bubble sort. */
    for (i = 0; i < reply->elements; ++i) {
        for (j = i+1; j < reply->elements; ++j) {
            if (strcmp(reply->element[i]->str, reply->element[j]->str) > 0) {
                tmp = reply->element[i]->str;
                ltmp = reply->element[i]->len;

                reply->element[i]->str = reply->element[j]->str;
                reply->element[i]->len = reply->element[j]->len;

                reply->element[j]->str = tmp;
                reply->element[j]->len = ltmp;
            }
        }
    }

    return IX_OK;
}

redisReply *smembers_from_redis(redisContext *redis_conn, const char *kbuf, int32_t klen, uint64_t ts, uint16_t num)
{
    redisReply *reply;

    log_info("redis[%d]-cmd [SMEMBERS %s]", num, kbuf);
    reply = redisCommand(redis_conn, "SMEMBERS %b", kbuf, klen);
    if (reply == NULL) {
        log_error("redis[%d]-cmd [SMEMBERS %s] error, time stamp:[%ld]", num, kbuf, ts);
        return NULL;
    }

    return reply;
}

int32_t smembers_from_redis_concat(redisContext *redis_conn, const char *kbuf, int32_t klen, char *vbuf, int32_t *vlen, uint64_t ts)
{
    int32_t ret = 0, cols = 0;
    int32_t i;

    *vlen = 0;

    /* get data from redis. */
    redisReply *reply = redisCommand(redis_conn, "SMEMBERS %s", kbuf);

    if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
        log_error("redis-cmd [SMEMBERS %s] error, time stamp:[%ld]", reply, ts);

        if (reply != NULL) {
            freeReplyObject(reply);
        }
        return -1;
    }

    if (reply->elements == 0) {
        log_warn("redis-cmd [SMEMBERS %s] no result, time stamp:[%ld]", kbuf, ts);

        freeReplyObject(reply);
        return 0;
    }

    /* check and sort the array, ASC. */
    ret = reply->elements;
    sort_array(reply);

    /* concat array's data to a string. */
    cols = get_column_count(reply->element[0]->str, '|');
    if (cols == 0) {
        log_error("column count [0] error, key [%s]", kbuf);
        freeReplyObject(reply);
        return -1;
    }

    sprintf(vbuf, "%ld*%d@", reply->elements, cols);
    *vlen = strlen(vbuf);
    for (i = 0; i < reply->elements; ++i) {

        /* check buffer size. */
        if (*vlen + reply->element[i]->len >= VAL_BUF_SIZE) {
            log_error("data size [%d] out of range, key[%s]", *vlen, kbuf);
            freeReplyObject(reply);
            return -1;
        }

        /* check column count. */
        if (cols != get_column_count(reply->element[i]->str, '|')) {

            log_error("column count [%d] error, key[%s], row [%s]", get_column_count(reply->element[i]->str, '|'), kbuf, reply->element[i]->str);
            // FIXME: urllog only logging error message, gps and activeruser must be abort and solove it.
            if (strstr(kbuf, "URL") == NULL) {
                freeReplyObject(reply);
                return -1;
            } 
        }

        /* copy data. */
        memcpy(vbuf + *vlen, reply->element[i]->str, reply->element[i]->len);
        *vlen += reply->element[i]->len;
        vbuf[(*vlen) ++] = '|';
    }

    freeReplyObject(reply);
    return ret;
}

int32_t set_to_tsdb(redisContext *tsdb_conn, const char *kbuf, int32_t klen, const char *vbuf, int32_t vlen, uint64_t ts)
{
    if (vlen == 0) {
        return IX_OK;
    }
    /* set data to tsdb. */
    redisReply *reply = redisCommand(tsdb_conn, "SET %s %b", kbuf, vbuf, vlen);

    if (reply == NULL || strcmp(reply->str, "OK") != 0) {
        log_error("tsdb-cmd [SET %s ...] error, time stamp:[%ld]", kbuf, ts);
        if (reply != NULL) {
            freeReplyObject(reply);
        }
        return IX_ERROR;
    }

    freeReplyObject(reply);
    return IX_OK;
}


int32_t expire_from_redis(redisContext *redis_conn, const char *kbuf, int32_t klen, uint64_t tl)
{
    redisReply *reply = redisCommand(redis_conn, "EXPIRE %s %d", kbuf, tl);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER) {
        log_error("tsdb-cmd [EXPIRE %s %d] error, time stamp:[%ld]", kbuf, tl);
        if (reply != NULL) {
            freeReplyObject(reply);
        }
        return IX_ERROR;
    }

    freeReplyObject(reply);
    return IX_OK;
}

int32_t handle_urlgps(redisContext *redis_conn, redisContext *tsdb_conn, backup_t *backup, const char *kbuf, int32_t klen, char *vbuf, int32_t *vlen, uint64_t ts, uint64_t tl)
{
    int32_t ret = smembers_from_redis_concat(redis_conn, kbuf, klen, vbuf, vlen, ts);

    if (ret > 0) {
        if (backup_set(backup, kbuf, klen, vbuf, *vlen) != IX_OK) {
            return -1;
        }

        if (set_to_tsdb(tsdb_conn, kbuf, klen, vbuf, *vlen, ts) != IX_OK) {
            return -1;
        }

        if (expire_from_redis(redis_conn, kbuf, klen, tl) != IX_OK) {
            return -1;
        }
    }

    return ret;
}

int16_t get_tsdb_index(const char *imei, int32_t len)
{
    if (len != 15) {
        log_error("imei:[%s] length is not 15.", imei);
        return 0;
    }

    int64_t i = atol(imei);
    return i % 2;
}

int32_t handle_activeuser(db_t *db, backup_t *backup, const char *kbuf, int32_t klen, char *vbuf, int32_t *vlen, uint64_t ts, uint64_t tl)
{
    int32_t i, j, cnt = 0, cols = 1;
    *vlen = 0;
    for (i = 0; i < db->redis_size; ++i) {
        if (db->reply[i]->type != REDIS_REPLY_ARRAY) {
            log_error("TODO: error");
            return -1;
        }

        cnt += db->reply[i]->elements;
    }

    if (cnt > 0) {
        sprintf(vbuf, "%d*%d@", cnt, cols);
        *vlen = strlen(vbuf);
        for (i = 0; i < db->redis_size; ++i) {

            for (j = 0; j < db->reply[i]->elements; ++j) {

                /* check buffer size. */
                if (*vlen + db->reply[i]->element[j]->len >= VAL_BUF_SIZE) {
                    log_error("data size [%d] out of range, key[%s]", *vlen, kbuf);
                    return -1;
                }

                /* check column count. */
                if (cols != get_column_count(db->reply[i]->element[j]->str, '|')) {
                    log_error("column count [%d] error, key[%s], row [%s]", get_column_count(db->reply[i]->element[j]->str, '|'), kbuf, db->reply[i]->element[j]->element[i]);
                    return -1;
                }

                /* copy data. */
                memcpy(vbuf + *vlen, db->reply[i]->element[j]->str, db->reply[i]->element[j]->len);
                *vlen += db->reply[i]->element[j]->len;
                vbuf[(*vlen) ++] = '|';
            }
        }

        if (backup_set(backup, kbuf, klen, vbuf, *vlen) != IX_OK) {
            return -1;
        }

        for (i = 0; i < db->tsdb_size; ++i) {
            if (set_to_tsdb(db->tsdb_conns[i], kbuf, klen, vbuf, *vlen, ts) != IX_OK) {
                return -1;
            }
        }

        for (i = 0; i < db->redis_size; ++i) {
            if (expire_from_redis(db->redis_conns[i], kbuf, klen, tl) != IX_OK) {
                return -1;
            }
        }
    }

    return cnt;
}



//int32_t smembers_handl
