/*
 * config.c
 *
 *  Created on: Nov 18, 2014
 *      Author: chenjf
 */


#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <json.h>

#include "config.h"
#include "logger.h"
#include "common.h"

cfg_info_t *cfg = NULL;

int32_t read_cfg(char *cfg_file)
{
    assert(cfg == NULL);
    cfg = (cfg_info_t *)malloc(sizeof(cfg_info_t));

    const char *str_val = NULL;
    struct json_object *obj = NULL;

    struct json_object *jsn = json_object_from_file(cfg_file);
    if (cfg == NULL) {
        goto read_cfg_fail;
    }

    /* redis_ip. */
    if (json_object_object_get_ex(jsn, "redis_ip", &obj)) {
        str_val = json_object_get_string(obj);
        cfg->redis_ip = x_strdup(str_val);
        log_out("config: redis_ip [%s]", cfg->redis_ip);
    } else {
        log_out("config: redis_ip not found");
        goto read_cfg_fail;
    }

    /* read_cfg_fail. */
    if (json_object_object_get_ex(jsn, "redis_port", &obj)) {
        cfg->redis_port = json_object_get_int(obj);
        log_out("config: redis_port [%d]", cfg->redis_port);
    } else {
        log_out("config: redis_port not found");
        goto read_cfg_fail;
    }

    /* tsdb_ip. */
    if (json_object_object_get_ex(jsn, "tsdb_ip", &obj)) {
        str_val = json_object_get_string(obj);
        cfg->tsdb_ip = x_strdup(str_val);
        log_out("config: tsdb_ip [%s]", cfg->tsdb_ip);
    } else {
        log_out("config: tsdb_ip not found");
        goto read_cfg_fail;
    }

    /* tsdb_port. */
    if (json_object_object_get_ex(jsn, "tsdb_port", &obj)) {
        cfg->tsdb_port = json_object_get_int(obj);
        log_out("config: tsdb_port [%d]", cfg->tsdb_port);
    } else {
        log_out("config: tsdb_port not found");
        goto read_cfg_fail;
    }

    /* back_path. */
    if (json_object_object_get_ex(jsn, "backup_path", &obj)) {
        str_val = json_object_get_string(obj);
        cfg->backup_path = x_strdup(str_val);
        log_out("config: backup_path [%s]", cfg->backup_path);
    } else {
        log_out("config: backup_path not found");
        goto read_cfg_fail;
    }

    if (json_object_object_get_ex(jsn, "stats_path", &obj)) {
        str_val = json_object_get_string(obj);
        cfg->stats_path = x_strdup(str_val);
        log_out("config: stats_path [%s]", cfg->stats_path);
    } else {
        log_out("config: stats_path not found");
        goto read_cfg_fail;
    }

    /* logger. */
    if (json_object_object_get_ex(jsn, "log_file", &obj)) {
        str_val = json_object_get_string(obj);
        cfg->log_file = x_strdup(str_val);
        log_out("config: log_file [%s]", cfg->log_file);
    } else {
        log_out("config: log_file not found");
        goto read_cfg_fail;
    }
    if (json_object_object_get_ex(jsn, "log_level", &obj)) {
        cfg->log_level = json_object_get_int(obj);
        log_out("config: log_level [%d]", cfg->log_level);
    } else {
        log_out("config: log_level not found");
        goto read_cfg_fail;
    }

    /* runtine. */
    if (json_object_object_get_ex(jsn, "start_time", &obj)) {
        cfg->start_time = json_object_get_int(obj);
        log_out("config: start_time [%d]", cfg->start_time);
    } else {
        log_out("config: start_time not found");
        goto read_cfg_fail;
    }
    if (json_object_object_get_ex(jsn, "time_limit", &obj)) {
        cfg->time_limit = json_object_get_int(obj);
        log_out("config: time_limit [%d]", cfg->time_limit);
    } else {
        log_out("config: time_limit not found");
        goto read_cfg_fail;
    }

    return IX_OK;

read_cfg_fail:
    log_out("invalid config file :%s", cfg_file);
    return IX_ERROR;
}
