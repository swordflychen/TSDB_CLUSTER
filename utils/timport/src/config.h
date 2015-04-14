/*
 * config.h
 *
 *  Created on: Nov 18, 2014
 *      Author: chenjf
 */

#pragma once

#include <stdint.h>

typedef struct _redis
{
    char *ip;
    int32_t port;
} redis_t;


typedef struct _cfg_info {
    /* database. */
    char        *redis_ip;

    redis_t     *redises;

    char        *tsdb_ip;
    int32_t     redis_port;
    int32_t     tsdb_port;

    /* backup&stats. */
    char        *backup_path;
    char        *stats_path;

    /* logs. */
    char        *log_file;
    int8_t      log_level;

    /* runtine. */
    int32_t     start_time;
    int32_t     time_limit;
} cfg_info_t;

int32_t read_cfg(char *cfg_file);
