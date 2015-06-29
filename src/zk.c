/*
 * file: zk.h
 * auth: cjfeii@126.com
 * date: Aug 8, 2014
 * desc: zookeeper access.
 */

#include <string.h>
#include <stdio.h>
#include <limits.h>

#include "zk.h"
#include "zookeeper.h"
#include "zookeeper_log.h"
#include "read_cfg.h"
#include "main.h"
#include "logger.h"

static zhandle_t *zk_handle = NULL;
static int zktimeout = 30000;

extern struct cfg_info g_cfg_pool;


/*
 * zk-server watcher.
 */
static void zk_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
    if(type == ZOO_SESSION_EVENT) {
        if(state == ZOO_CONNECTED_STATE) {
            log_info("connected to zookeeper service successfully!");
        }
        else if (state == ZOO_EXPIRED_SESSION_STATE){
            log_fatal("zookeeper session expired!");
            /* TODO: exit or reconnect? */
            raise(SIGUSR1);
        }
    }
}

/*
 * znode watcher.
 */
static void znode_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
    if(state == ZOO_CONNECTED_STATE) {
        if (type == ZOO_DELETED_EVENT) {
            log_fatal("znode [%s] has been deleted on the zk-server.", path);
            raise(SIGUSR1);
        }
    }
}

/*
 * create a znode if the znode is not exists.
 */
static int not_exists_and_create(const char *node, const char *value, const int flags)
{
    int ret = 0;
    ret = zoo_exists(zk_handle, node, 0, NULL);
    if(ZOK != ret) {
        ret = zoo_create(zk_handle, node, value, strlen(value), &ZOO_OPEN_ACL_UNSAFE, flags, NULL, 0);
        if(ZOK != ret) {
            log_fatal("create znode [%s] error, errno:%d", node, ret);
            return -1; /* create error. */
        }
    } else {
        log_info("znode [%s] already exists.", node);
        return 1; /* this node already exists. */
    }

    return 0; /* create node ok. */
}

int zk_init(const char *cxt)
{
    log_info("initial zookeeper.");

    /* set zk log level. */
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);

    /* create a zk handle. */
    zk_handle = zookeeper_init(g_cfg_pool.zk_server, zk_watcher, zktimeout, 0, (void *)cxt, 0);
    if(!zk_handle) {
        log_fatal("zookeeper_init zk_server [%s] error", g_cfg_pool.zk_server);
        return -1;
    }

    /*
     * create keys znode.
     * znode: /keys
     */
    if(not_exists_and_create("/keys", "tsdb keys.", 0) < 0) {
        return -1;
    }

#if 0
    /*
     * create datasets znode.
     * znode: /datasets
     */
    if(not_exists_and_create("/datasets", "tsdb datasets.", 0) < 0) {
        return -1;
    }
#endif

    return 0;
}

void zk_close()
{
    log_info("close zookeeper.");
    zookeeper_close(zk_handle);
    zk_handle = NULL;
}

int zk_register_node(void)
{
    log_info("register the data node to zookeeper.");

    int ret = 0;
    char node[PATH_MAX] = {0};
    char value[1024] = {0};
    char role[8] = {0};

#if 0
    /*
     * create a data set znode.
     * znode: /datasets/DS1
     */
    sprintf(node, "/datasets/%s", g_cfg_pool.ds_id);
    if(not_exists_and_create(node, g_cfg_pool.ds_id, 0) < 0) {
        return -1;
    }


    /*
     * create znode for the data node, name like this: /datasets/DS1/ip:wport:rport .
     * znode: /datasets/DS1/RW:127.0.0.1:7001:7002:20120101000000:-1
     * value: mode:RW; ds_id:DS2; key:0; time_range:[20120101000000, -1];
     */
    sprintf(node + strlen(node), "/%s:%s:%d:%d:%ld:%ld",
            g_cfg_pool.mode, g_cfg_pool.ip, g_cfg_pool.w_port, g_cfg_pool.r_port, g_cfg_pool.start_time, g_cfg_pool.end_time);
//    sprintf(value, "mode:%s; ds_id:%s; key:%ld; time_range:[%ld, %ld];",
//            g_cfg_pool.mode,g_cfg_pool.ds_id,g_cfg_pool.key, g_cfg_pool.start_time, g_cfg_pool.end_time);
    if(not_exists_and_create(node, "", ZOO_EPHEMERAL) != 0) {
        return -1;
    }

    /* watch the znode. */
    log_debug("watch znode: [%s].", node);
    ret = zoo_wexists(zk_handle, node, znode_watcher, "watch this node.", NULL);
    if(ret != ZOK) {
        log_error("zoo_wexists znode [%s] error, errno:%d", node, ret);
        return -1;
    }
#endif

    /*
     * create znode for key.
     * znode: /keys/0:2048
     */
    sprintf(node, "/keys/%ld:%ld", g_cfg_pool.key_start, g_cfg_pool.key_end);
    sprintf(value, "key_set:[%ld, %ld]", g_cfg_pool.key_start, g_cfg_pool.key_end);
    if(not_exists_and_create(node, value, 0) < 0) {
        return -1;
    }


    /*
     * create znodes for data node.
     * znode: /keys/0:2048/RO:1000:MASTER:192.168.1.10:7501:7502:20130101000000:20140101000000
     * or:    /keys/0:2048/RW:1001:SLAVE:192.168.1.14:7501:7502:20140101000000:-1
     * value: data_id:DS1; ...
     */
    if (g_cfg_pool.role == MASTER) {
        strcpy(role, ":MASTER");
    } else if (g_cfg_pool.role == SLAVE) {
        strcpy(role, ":SLAVE");
    } else {
        strcpy(role, ":SINGLE");
    }
    sprintf(node + strlen(node), "/%s:%d%s:%s:%d:%d:%ld:%ld", g_cfg_pool.mode, g_cfg_pool.ds_id, role,
            g_cfg_pool.ip, g_cfg_pool.w_port, g_cfg_pool.r_port, g_cfg_pool.start_time, g_cfg_pool.end_time);
    sprintf(value, "ds_id:%d; ip:%s; write_port:%d; read_port:%d; mode:%s; key_set:[%ld, %ld]; time_range:[%ld, %ld];",
            g_cfg_pool.ds_id, g_cfg_pool.ip, g_cfg_pool.w_port, g_cfg_pool.r_port, g_cfg_pool.mode,
            g_cfg_pool.key_start, g_cfg_pool.key_end, g_cfg_pool.start_time, g_cfg_pool.end_time);
    if (not_exists_and_create(node, value, ZOO_EPHEMERAL) < 0) {
        return -1;
    }

    /* watch the znode. */
    log_debug("watch znode: [%s].", node);
    ret = zoo_wexists(zk_handle, node, znode_watcher, "watch this node.", NULL);
    if(ret != ZOK) {
        log_error("zoo_wexists znode [%s] error, errno:%d", node, ret);
        return -1;
    }

    return 0;
}

int zk_unregister_node(void)
{
    log_info("unregister the data node to zookeeper.");
    // TODO something.
    // zookeeper_close(zk_handle);
    return 0;
}

