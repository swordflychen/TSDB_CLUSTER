/*
 * statistics.c
 *
 *  Created on: Nov 18, 2014
 *      Author: chenjf
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "statistics.h"
#include "common.h"
#include "logger.h"

//statistics_t *stats = NULL;

statistics_t *statistics_open(const char *path, const database_t *redis)
{
    /* allocate memory. */
    statistics_t *stats = (statistics_t *)malloc(sizeof(statistics_t));
    strcpy(stats->fname, path);

    /* open statistics file. */
    stats->fd = open(path, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
    if (stats->fd == -1) {
        log_error("open statistics file [%s] error, errno [%d]", path, errno);
        return NULL;
    }

    stats->conn = redisConnect(redis->ip, redis->port);
    if (stats->conn == NULL || stats->conn->err) {
        if (stats->conn != NULL) {
            log_error("Connection error: %s", stats->conn->errstr);
        } else {
            log_error("Connection error: can't allocate statistics redis context");
        }
        return NULL;
    }

    return stats;
}

int32_t statistics_close(statistics_t *stats)
{
    /* free memory.*/
    if (stats != NULL) {

        redisFree(stats->conn);

        close(stats->fd);

        free(stats);
    }
    return IX_OK;
}

int32_t statistics_set_urllog(statistics_t *stats, const char *pattern, const char *tt)
{
    if (stats == NULL || stats->urllog.is_set == 0) {
        return IX_OK;
    }

    /* write logs. */
    char line[STAT_LEN] = {0};
    sprintf(line, "%s;%s;act_cnt:%10d;url_cnt:%10d;gps_cnt:%10d;act_len:%10d;url_len:%10d;gps_len:%10d;\n",
            pattern, tt, stats->urllog.act_cnt, stats->urllog.url_cnt, stats->urllog.gps_cnt, stats->urllog.act_len, stats->urllog.url_len, stats->urllog.gps_len);

    if( write_file(stats->fd, line, strlen(line)) != IX_OK) {
        log_error("write statistics file [%s] error, errno [%d]", stats->fname, errno);
        return IX_ERROR;
    }

    /* set redis, TODO: EVAL-cmd. */
    char date[10] = {0};
    memcpy(date, tt, 8);    // 20141216

    /* gps count. */
    redisReply *reply = redisCommand(stats->conn, "INCRBY %s:gpsCount %d", date, stats->urllog.gps_cnt);
    if (reply == NULL) {
        log_error("redis-cmd:[INCRBY %s:gpsCount %d] error", date, stats->urllog.gps_cnt);
        return IX_ERROR;
    }
    freeReplyObject(reply);

    /* url count. */
    reply = redisCommand(stats->conn, "INCRBY %s:urlCount %d", date, stats->urllog.url_cnt);
    if (reply == NULL) {
        log_error("redis-cmd:[INCRBY %s:urlCount %d] error", date, stats->urllog.url_cnt);
        return IX_ERROR;
    }
    freeReplyObject(reply);

    /* gps size. */
    reply = redisCommand(stats->conn, "INCRBY %s:gpsSize %d", date, stats->urllog.gps_len);
    if (reply == NULL) {
        log_error("redis-cmd:[INCRBY %s:gpsSize %d] error", date, stats->urllog.gps_len);
        return IX_ERROR;
    }
    freeReplyObject(reply);

    /* url size. */
    reply = redisCommand(stats->conn, "INCRBY %s:urlSize %d", date, stats->urllog.url_len);
    if (reply == NULL) {
        log_error("redis-cmd:[INCRBY %s:urlSize %d] error", date, stats->urllog.url_len);
        return IX_ERROR;
    }
    freeReplyObject(reply);



    return IX_OK;
}



