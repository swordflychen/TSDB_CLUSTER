#include <stdlib.h>
#include <string.h>
#include <json.h>

#include "read_cfg.h"
#include "main.h"
#include "utils.h"
#include "logger.h"

void read_cfg(struct cfg_info *p_cfg, char *cfg_file)
{
    const char *str_val = NULL;
    struct json_object *obj = NULL, *tmp_obj = NULL;
    struct json_object *cfg = json_object_from_file(cfg_file);
    int array_len = 0;
    if (cfg == NULL) {
        goto fail;
    }

    /* node type. */
    if (json_object_object_get_ex(cfg, "node_type", &obj)) {
        str_val = json_object_get_string(obj);
        if (strcmp(str_val, "SINGLE") == 0) {
            p_cfg->node_type = SINGLE;
        } else if (strcmp(str_val, "CLUSTER") == 0) {
            p_cfg->node_type = CLUSTER;
        } else {
            log_out("config: node_type error");
            goto fail;
        }
    } else {
        log_out("config: node_type not found");
        goto fail;
    }

    /* mode. */
    if (json_object_object_get_ex(cfg, "mode", &obj)) {
        str_val = json_object_get_string(obj);
        p_cfg->mode = x_strdup(str_val);
    } else {
        log_out("config: mode not found");
        goto fail;
    }
    if (strcmp(p_cfg->mode, "RO") == 0 ) {
        p_cfg->ldb_readonly_switch = 1;
    } else if (strcmp(p_cfg->mode, "RW") == 0) {
        p_cfg->ldb_readonly_switch = 0;
    } else {
        log_out("config: mode error");
        goto fail;
    }

    /* data set id. */
    if (json_object_object_get_ex(cfg, "ds_id", &obj)) {
        p_cfg->ds_id = json_object_get_int(obj);
    } else {
        log_out("config: ds_id not found");
        goto fail;
    }

    /* key set 2 */
    if (json_object_object_get_ex(cfg, "key_set", &obj)) {
        if (json_object_array_length(obj) != 2) {
            log_out("config: key_set2 error");
            goto fail;
        }

        tmp_obj = json_object_array_get_idx(obj, 0);
        p_cfg->key_start = json_object_get_int(tmp_obj);
        tmp_obj = json_object_array_get_idx(obj, 1);
        p_cfg->key_end = json_object_get_int(tmp_obj);
    } else {
        log_out("config: key_set not found");
        goto fail;
    }

    /* time range. */
    if (json_object_object_get_ex(cfg, "time_range", &obj)) {
        array_len = json_object_array_length(obj);
        if (array_len != 2) {
            log_out("config: time_range error");
            goto fail;
        }

        tmp_obj = json_object_array_get_idx(obj, 0);
        p_cfg->start_time = json_object_get_int64(tmp_obj);

        tmp_obj = json_object_array_get_idx(obj, 1);
        p_cfg->end_time = json_object_get_int64(tmp_obj);
    } else {
        log_out("config: time_range not found");
        goto fail;
    }
    if (p_cfg->ldb_readonly_switch == 1) {
        if (p_cfg->start_time > p_cfg->end_time) {
            log_out("config: start_time > end_time");
            goto fail;
        }
    } else if (p_cfg->ldb_readonly_switch == 0) {
        if (p_cfg->end_time != -1 ) {
            log_out("config: end_time error");
            goto fail;
        }
    }

    /* zookeeper server. */
    if (json_object_object_get_ex(cfg, "zk_server", &obj)) {
        str_val = json_object_get_string(obj);
        p_cfg->zk_server = x_strdup(str_val);
    } else {
        log_out("config: zk_server not found");
        goto fail;
    }

    /* IP address. */
    if (json_object_object_get_ex(cfg, "ip", &obj)) {
        str_val = json_object_get_string(obj);
        p_cfg->ip = x_strdup(str_val);
    } else {
        log_out("config: ip not found");
        goto fail;
    }

    /* write and read port. */
    if (json_object_object_get_ex(cfg, "w_port", &obj)) {
        p_cfg->w_port = json_object_get_int(obj);
    } else {
        log_out("config: w_port not found");
        goto fail;
    }
    if (json_object_object_get_ex(cfg, "r_port", &obj)) {
        p_cfg->r_port = json_object_get_int(obj);
    } else {
        log_out("config: r_port not found");
        goto fail;
    }

    /* max connect count. */
    if (json_object_object_get_ex(cfg, "max_connect", &obj)) {
        p_cfg->max_connect = json_object_get_int64(obj);
    } else {
        log_out("config: max_connect not found");
        goto fail;
    }
    if (json_object_object_get_ex(cfg, "num_threads", &obj)) {
        p_cfg->num_threads = json_object_get_int(obj);
    } else {
        log_out("config: num_threads not found");
        goto fail;
    }

    /* workspace directory. */
    if (json_object_object_get_ex(cfg, "work_path", &obj)) {
        str_val = json_object_get_string(obj);
        p_cfg->work_path = x_strdup(str_val);
    } else {
        log_out("config: work_path not found");
        goto fail;
    }

    /* log configure. */
    if (json_object_object_get_ex(cfg, "log_path", &obj)) {
        str_val = json_object_get_string(obj);
        p_cfg->log_path = x_strdup(str_val);
    } else {
        log_out("config: log_path not found");
        goto fail;
    }
    if (json_object_object_get_ex(cfg, "log_file", &obj)) {
        str_val = json_object_get_string(obj);
        p_cfg->log_file = x_strdup(str_val);
    } else {
        log_out("config: log_file not found");
        goto fail;
    }
    if (json_object_object_get_ex(cfg, "log_level", &obj)) {
        p_cfg->log_verbosity = json_object_get_int(obj);
    } else {
        log_out("config: log_level not found");
        goto fail;
    }

    /* ldb configure. */
    if (json_object_object_get_ex(cfg, "ldb_write_buffer_size", &obj)) {
        p_cfg->ldb_write_buffer_size = json_object_get_int64(obj);
    } else {
        log_out("config: ldb_write_buffer_size not found");
        goto fail;
    }

    if (json_object_object_get_ex(cfg, "ldb_block_size", &obj)) {
        p_cfg->ldb_block_size = json_object_get_int64(obj);
    } else {
        log_out("config: ldb_block_size not found");
        goto fail;
    }

    if (json_object_object_get_ex(cfg, "ldb_cache_lru_size", &obj)) {
        p_cfg->ldb_cache_lru_size = json_object_get_int64(obj);
    } else {
        log_out("config: ldb_cache_lru_size not found");
        goto fail;
    }

    if (json_object_object_get_ex(cfg, "ldb_bloom_key_size", &obj)) {
        p_cfg->ldb_bloom_key_size = json_object_get_int(obj);
    } else {
        log_out("config: ldb_bloom_key_size not found");
        goto fail;
    }

    if (json_object_object_get_ex(cfg, "ldb_compaction_speed", &obj)) {
        p_cfg->ldb_compaction_speed = json_object_get_int(obj);
    } else {
        log_out("config: ldb_compaction_speed not found");
        goto fail;
    }

    /* slave. */
    if (json_object_object_get_ex(cfg, "has_slave", &obj)) {
        p_cfg->has_slave = json_object_get_int(obj);
    } else {
        log_out("config: has_slave not found");
        goto fail;
    }
    if (p_cfg->has_slave != 0) {
        if (json_object_object_get_ex(cfg, "role", &obj)) {
            str_val = json_object_get_string(obj);
            if (strcmp(str_val, "MASTER") == 0) {
                p_cfg->role = MASTER;
            } else if (strcmp(str_val, "SLAVE") == 0) {
                p_cfg->role = SLAVE;
            } else {
                log_out("config: role config error");
                goto fail;
            }
        } else {
            log_out("config: role not found");
            goto fail;
        }

        if (json_object_object_get_ex(cfg, "slave_ip", &obj)) {
            str_val = json_object_get_string(obj);
            p_cfg->slave_ip = x_strdup(str_val);
        } else {
            log_out("config: slave_ip not found");
            goto fail;
        }
        if (json_object_object_get_ex(cfg, "slave_wport", &obj)) {
            p_cfg->slave_wport = json_object_get_int(obj);
        } else {
            log_out("config: slave_wport not found");
            goto fail;
        }

        log_debug("slave ip:[%s]", p_cfg->slave_ip);
        log_debug("slave wport:[%d]", p_cfg->slave_wport);
    }

    return;
fail:
    log_out("invalid config file :%s", cfg_file);
    exit(EXIT_FAILURE);
}
