/*
 * export.c
 *
 *  Created on: Nov 12, 2014
 *      Author: chenjf
 */

#include <assert.h>
#include <stdlib.h>

#include "timport.h"
#include "common.h"
#include "db.h"
#include "statistics.h"

timport_t *timport = NULL;

int32_t timport_open(const char *stats_path, const char *backup_path, const database_t *redis, int32_t redis_size, const database_t *tsdb, const int16_t tsdb_size, const database_t *stats)
{
    assert(timport == NULL);
    timport = (timport_t *)calloc(1, sizeof(timport_t));
    assert(timport != NULL);

    /* open statistics. */
    timport->stats = statistics_open(stats_path, stats);
    assert(timport->stats != NULL);

    /* open backup file. */
    timport->backup = backup_open(backup_path);
    assert(timport->backup != NULL);

    /* open open database. */
    timport->db = db_open(redis, redis_size, tsdb, tsdb_size);
    assert(timport->db != NULL);

    return IX_OK;
}

int32_t timport_close(void)
{
    if (timport != NULL) {
        /* close backup. */
        backup_close(timport->backup);

        /* close database. */
        db_close(timport->db);

        /* close statistics. */
        statistics_close(timport->stats);

        /* close timport. */
        free(timport);
        timport = NULL;
    }

    return IX_OK;
}


int32_t timport_urllog(int64_t ts, int64_t tl)
{
    int32_t i, j, ret;
    int16_t index;

    /* generate time interval. */
    char tt[32] = {0};
    strftime(tt, sizeof(tt), "%Y%m%d%H%M", localtime(&ts));
    tt[strlen(tt) - 1] = 0;

    memset(&timport->stats->urllog, 0, sizeof(timport->stats->urllog));

    for (i = 0; i < timport->db->redis_size; ++i) {
        /* get active users from redis. */
        sprintf(timport->kbuf, "ACTIVEUSER:%s", tt);
        timport->db->reply[i] = smembers_from_redis(timport->db->redis_conns[i], timport->kbuf, strlen(timport->kbuf), ts, i);
        if (timport->db->reply[i] == NULL) {
            log_error("smembers_from_redis [%d] error.", i);
            return IX_ERROR;
        }

        /* loop all the active users. */

        if (timport->db->reply[i]->type == REDIS_REPLY_ARRAY) {
            for (j = 0; j < timport->db->reply[i]->elements; ++j) {

                index = get_tsdb_index(timport->db->reply[i]->element[j]->str, timport->db->reply[i]->element[j]->len);

                /* deal url log. */
                sprintf(timport->kbuf, "URL:%s:%s", timport->db->reply[i]->element[j]->str, tt);
                ret = handle_urlgps(timport->db->redis_conns[i], timport->db->tsdb_conns[index], timport->backup, timport->kbuf, strlen(timport->kbuf), timport->vbuf, &timport->vbuf_len, ts, 1800);
                if (ret < 0) {
                    return IX_ERROR;
                }
                timport->stats->urllog.url_cnt += ret;
                timport->stats->urllog.url_len += timport->vbuf_len;


                /* deal gps.*/
                sprintf(timport->kbuf, "GPS:%s:%s", timport->db->reply[i]->element[j]->str, tt);
                ret = handle_urlgps(timport->db->redis_conns[i], timport->db->tsdb_conns[index], timport->backup, timport->kbuf, strlen(timport->kbuf), timport->vbuf, &timport->vbuf_len, ts, tl);
                if ( ret < 0) {
                    return IX_ERROR;
                }
                timport->stats->urllog.gps_cnt += ret;
                timport->stats->urllog.gps_len += timport->vbuf_len;
            }
        }
    }

    /* merge active user. */
    sprintf(timport->kbuf, "ACTIVEUSER:%s", tt);
    ret = handle_activeuser(timport->db, timport->backup, timport->kbuf, strlen(timport->kbuf), timport->vbuf, &timport->vbuf_len, ts, tl);
    if ( ret < 0) {
        return IX_ERROR;
    }
    timport->stats->urllog.act_cnt += ret;
    timport->stats->urllog.act_len += timport->vbuf_len;
    timport->stats->urllog.is_set = 1;

    /* write statistics. */
    if (statistics_set_urllog(timport->stats, "URLLOG", tt) != IX_OK) {
        return IX_ERROR;
    }

    /* free memory. */
    for (i = 0; i < timport->db->redis_size; ++i) {
        freeReplyObject(timport->db->reply[i]);
    }

    return IX_OK;
}


int32_t timport_exurllog(int64_t ts, int64_t tl)
{
    int32_t i, j, ret;
    int16_t index;

    /* generate time interval. */
    char tt[32] = {0};
    strftime(tt, sizeof(tt), "%Y%m%d%H%M", localtime(&ts));
    tt[strlen(tt) - 1] = 0;

    memset(&timport->stats->urllog, 0, sizeof(timport->stats->urllog));
    for (i = 0; i < timport->db->redis_size; ++i) {
        /* get active users from redis. */
        sprintf(timport->kbuf, "RACTIVEUSER:%s", tt);
        timport->db->reply[i] = smembers_from_redis(timport->db->redis_conns[i], timport->kbuf, strlen(timport->kbuf), ts, i);
        if (timport->db->reply[i] == NULL) {
            log_error("smembers_from_redis [%d] error.", i);
            return IX_ERROR;
        }

        /* loop all the active users. */

        if (timport->db->reply[i]->type == REDIS_REPLY_ARRAY) {
            for (j = 0; j < timport->db->reply[i]->elements; ++j) {

                index = get_tsdb_index(timport->db->reply[i]->element[j]->str, timport->db->reply[i]->element[j]->len);

                /* deal url log. */
                sprintf(timport->kbuf, "URL:%s:%s", timport->db->reply[i]->element[j]->str, tt);
                ret = handle_urlgps(timport->db->redis_conns[i], timport->db->tsdb_conns[index], timport->backup, timport->kbuf, strlen(timport->kbuf), timport->vbuf, &timport->vbuf_len, ts, 1800);
                if (ret < 0) {
                    return IX_ERROR;
                }
                timport->stats->urllog.url_cnt += ret;
                timport->stats->urllog.url_len += timport->vbuf_len;


                /* deal gps.*/
                sprintf(timport->kbuf, "RGPS:%s:%s", timport->db->reply[i]->element[j]->str, tt);
                ret = handle_urlgps(timport->db->redis_conns[i], timport->db->tsdb_conns[index], timport->backup, timport->kbuf, strlen(timport->kbuf), timport->vbuf, &timport->vbuf_len, ts, tl);
                if ( ret < 0) {
                    return IX_ERROR;
                }
                timport->stats->urllog.gps_cnt += ret;
                timport->stats->urllog.gps_len += timport->vbuf_len;
            }
        }
    }

    /* merge active user. */
    sprintf(timport->kbuf, "RACTIVEUSER:%s", tt);
    ret = handle_activeuser(timport->db, timport->backup, timport->kbuf, strlen(timport->kbuf), timport->vbuf, &timport->vbuf_len, ts, tl);
    if ( ret < 0) {
        return IX_ERROR;
    }
    timport->stats->urllog.act_cnt += ret;
    timport->stats->urllog.act_len += timport->vbuf_len;
    timport->stats->urllog.is_set = 1;

    /* write statistics. */
    if (statistics_set_urllog(timport->stats, "RURLLOG", tt) != IX_OK) {
        return IX_ERROR;
    }

    /* free memory. */
    for (i = 0; i < timport->db->redis_size; ++i) {
        freeReplyObject(timport->db->reply[i]);
    }

    return IX_OK;
}
