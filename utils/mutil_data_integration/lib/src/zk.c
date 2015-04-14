/*
 * file: zk.c
 * date: 2014-08-04
 * auth: chenjianfei@daoke.me
 * desc: zk.c
 */


#include <string.h>
#include <stdio.h>


#include "zk.h"

/*
 * zk-server watcher.
 */
static void zk_watcher(zhandle_t *zh, int type, int state, const char *path, void *zk_context)
{
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            printf("[luazk][zk_watcher]:connected to zookeeper service successfully!\n");
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            printf("[luazk][zk_watcher]:Zookeeper session expired!\n");

            zk_cxt_t *zc = (zk_cxt_t *) zk_context;
            zc->zkhandle = zookeeper_init(zc->zkhost, zk_watcher, 30000, 0, zc, 0);

            // FIXME: need double check.
            exit(1);
        }
    }
}

/*
 * watch: "/keys"
 */
static void keys_watcher(zhandle_t *zh, int type, int state, const char *path, void *zk_cxt)
{
    printf("[luazk][znode_watcher]:type: %d, state: %d, path: %s\n", type, state, path);

    if (state == ZOO_CONNECTED_STATE) {
        /* we watched znode is changed. */
        if (type == ZOO_CHILD_EVENT) {
            zk_cxt_t *zc = (zk_cxt_t *)zk_cxt;
            zc->key_cnt = 0;

            // need register again FIXME: check.
            register_zkcache(zc);
        }
    }

    return;
}

/*
 * watch: "/keys/0:2048" ...
 */
static void keyset_znode_watcher(zhandle_t *zh, int type, int state, const char *path, void *key_set)
{
    if (state == ZOO_CONNECTED_STATE) {
        if (type == ZOO_CHILD_EVENT) {
            key_set_t *ks = (key_set_t *) key_set;
            ks->is_synced = 0;
            ks->ds_cnt = 0;
            bzero(ks->data_set, sizeof(ks->data_set));
        }
    }
}

zk_cxt_t *open_zkhandler(const char *zkhost)
{
    /* alloc zk context. */
    zk_cxt_t *zc = (zk_cxt_t *)malloc(sizeof(zk_cxt_t));
    if (zc == NULL) {
        return NULL;
    }
    bzero(zc, sizeof(zk_cxt_t));

    /* set zk log level. */
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    strcpy(zc->zkhost, zkhost);

    /* open a zookeeper. */
    zc->zkhandle = zookeeper_init(zkhost, zk_watcher, 30000, 0, zc, 0);
    if (zc->zkhandle == NULL ) {
        free(zc);
        return NULL;
    }

    return zc;
}

int close_zkhandler(zk_cxt_t *zc)
{
    /* check handle. */
    if (zc == NULL || zc->zkhandle == NULL) {
        return -1;
    }

    /* close zk-server. */
    zookeeper_close(zc->zkhandle);

    /* free resource. */
    free(zc);

    return 0;
}

/*
 * keynode: "0:2048"
 */
static int parse_keynode(const char *node, uint64_t *sk, uint64_t *ek)
{
    uint64_t tmp;
    char *stop = NULL;
    *sk = strtoll(node, &stop, 10);
    if (stop == NULL) {
        return -1;
    }

    *ek = strtoll(strchr(node, ':') + 1, &stop, 10);
    if (stop == NULL) {
        return -1;
    }

    return 0;
}
int register_zkcache(zk_cxt_t *zc)
{
    /* check handle. */
    if (zc == NULL || zc->zkhandle == NULL) {
        return -1;
    }

    /* no need refresh. */
    if (zc->key_cnt != 0) {
        return 0;
    }

    /* clean the keysets. */
    bzero(zc->key_set, sizeof(zc->key_set));

    /*
     * get "/keys" children and set watcher.
     */
    struct String_vector sv;
    int32_t ret = zoo_wget_children(zc->zkhandle, "/keys", keys_watcher, zc, &sv);
    if (ret != ZOK || sv.count == 0) {
        return -1;
    }

    /* set keysets. */
    int32_t i;
    for (i=0; i<sv.count; ++i) {
        sprintf(zc->key_set[i].path, "/keys/%s", sv.data[i]);
        ret = parse_keynode(sv.data[i], &(zc->key_set[i].s_key), &(zc->key_set[i].e_key));
        if (ret == -1) {
            return -1;
        }
        zc->key_cnt ++;
    }

    return 0;
}

int unregister_zkcache(zk_cxt_t *zc)
{
    zc->key_cnt = 0;
    return 0;
}


#define get_field() \
        p = strchr(p, ':') + 1; \
        if (p == NULL) return -1; \
        snprintf(tmp, strchr(p, ':') - p + 1, "%s", p);

/*
 * node: RW:1:MASTER:192.168.1.12:7503:7504:20140101000000:-1
 * or:   RO:1:SLAVE:192.168.1.12:7503:7504:20130101000000:20140101000000
 */
static int parser_datanode(const char *node, data_set_t *data_set)
{
    char tmp[64] = { 0 };
    const char *p2;

    /* mode */
    const char *p = node;
    if (strstr(p, "RW:") != NULL) {
        data_set->mode = RW;
    } else if (strstr(p, "RO:") != NULL) {
        data_set->mode = RO;
    } else {
        return -1;
    }

    /* ds_id */
    get_field();
    data_set->id = atoi(tmp);

    /* role */
    get_field();
    if (strcmp(tmp, MASTER) != 0) {
        data_set->data_node[0].role = MASTER;
    } else if(strcmp(tmp, SLAVE) != 0) {
        data_set->data_node[0].role = SLAVE;
    } else {
        data_set->data_node[0].role = ERRROLE;
    }

    /* ip */
    get_field();
    strcpy(data_set->data_node[0].ip, tmp);

    /* write port */
    get_field();;
    data_set->data_node[0].w_port = atoi(tmp);

    /* read port */
    get_field();
    data_set->data_node[0].r_port = atoi(tmp);

    /* start time */
    get_field();
    data_set->s_time = atoll(tmp);

    /* end time */
    get_field();
    data_set->e_time = atoll(tmp);

    return 0;
}

/*
 * path: /keys/0:1024
 */
int sync_keyset(zhandle_t *zkhandle, const char *path, key_set_t *key_set)
{
    /* check args. */
    if (key_set == NULL || key_set->is_synced != 0) {
        return 0;
    }

    /* get keys's children node and set watcher. */
    struct String_vector sv;
    int32_t ret = zoo_wget_children(zkhandle, path, keyset_znode_watcher, key_set, &sv);
    if (ret != ZOK) {
        return -1;
    }

    int i, offset, flag = 1;
    data_set_t tmp_ds;
    for (i=0; i<sv.count; ++i) {
        bzero(&tmp_ds, sizeof(data_set_t));
        ret = parser_datanode(sv.data[i], &tmp_ds);
        if (ret == -1) {
            return -1;
        }

        /* set read/write dataset。 */
        if (tmp_ds.mode == RW) {
            /* RW dataset. */
            if (key_set->data_set[0].dn_cnt == 0) {
                key_set->data_set[0].mode = RW;
                key_set->data_set[0].dn_cnt ++;     // 1
                key_set->data_set[0].id = tmp_ds.id;

                key_set->data_set[0].s_time = tmp_ds.s_time;
                key_set->data_set[0].e_time = tmp_ds.e_time;

                strcpy(key_set->data_set[0].path, sv.data[i]);
                strcpy(key_set->data_set[0].data_node[0].ip, tmp_ds.data_node[0].ip);
                key_set->data_set[0].data_node[0].w_port = tmp_ds.data_node[0].w_port;
                key_set->data_set[0].data_node[0].r_port = tmp_ds.data_node[0].r_port;
                key_set->data_set[0].data_node[0].role = tmp_ds.data_node[0].role;

                key_set->ds_cnt ++;     // add dataset count at once insert.
                flag = 0;
            } else if (key_set->data_set[0].dn_cnt < DN_PER_DS) {
                offset = key_set->data_set[0].dn_cnt;
                if (tmp_ds.id != key_set->data_set[0].id) {
                    return -1;
                }
                strcpy(key_set->data_set[0].data_node[ key_set->data_set[0].dn_cnt ].ip, tmp_ds.data_node[0].ip);
                key_set->data_set[0].data_node[ offset ].w_port = tmp_ds.data_node[0].w_port;
                key_set->data_set[0].data_node[ offset ].r_port = tmp_ds.data_node[0].r_port;
                key_set->data_set[0].data_node[ offset ].role = tmp_ds.data_node[0].role;

                key_set->data_set[0].dn_cnt ++;
            } else {
                return -1;
            }

        } else {
            /* set RO dataset. */
            offset = key_set->ds_cnt + flag;
            if (key_set->data_set[offset].dn_cnt == 0) {
                key_set->data_set[offset].mode = RO;
                key_set->data_set[offset].dn_cnt ++;    // 1
                key_set->data_set[offset].id = tmp_ds.id;

                key_set->data_set[offset].s_time = tmp_ds.s_time;
                key_set->data_set[offset].e_time = tmp_ds.e_time;

                strcpy(key_set->data_set[offset].path, sv.data[i]);
                strcpy(key_set->data_set[offset].data_node[0].ip, tmp_ds.data_node[0].ip);
                key_set->data_set[offset].data_node[offset].w_port = tmp_ds.data_node[0].w_port;
                key_set->data_set[offset].data_node[offset].r_port = tmp_ds.data_node[0].r_port;
                key_set->data_set[offset].data_node[offset].role = tmp_ds.data_node[0].role;

                key_set->ds_cnt ++;     // add dataset count at once insert.
            } else if (key_set->data_set[offset].dn_cnt < DN_PER_DS) {
                offset = key_set->data_set[offset].dn_cnt;
                if (tmp_ds.id != key_set->data_set[offset].id) {
                    return -1;
                }
                strcpy(key_set->data_set[offset].data_node[ key_set->data_set[0].dn_cnt ].ip, tmp_ds.data_node[0].ip);
                key_set->data_set[offset].data_node[ key_set->data_set[0].dn_cnt ].w_port = tmp_ds.data_node[0].w_port;
                key_set->data_set[offset].data_node[ key_set->data_set[0].dn_cnt ].r_port = tmp_ds.data_node[0].r_port;
                key_set->data_set[offset].data_node[ key_set->data_set[0].dn_cnt ].role = tmp_ds.data_node[0].role;

                key_set->data_set[0].dn_cnt ++;
            } else {
                return -1;
            }
        }
    }

    return 0;
}

int get_write_dataset(zk_cxt_t *zc, uint64_t key_field, const data_set_t **ds)
{
    /* check handle. */
    if (zc == NULL || zc->zkhandle == NULL) {
        return -1;
    }

    /* check valid. */
    if (zc->key_cnt == 0) {
        return -1;
    }

    int i;
    for (i=0; i<zc->key_cnt; ++i) {
        if (key_field >= zc->key_set[i].s_key && key_field < zc->key_set[i].e_key) {
            if (zc->key_set[i].is_synced == 0) {
                /* need fresh. */
                if (-1 == sync_keyset(zc->zkhandle, zc->key_set[i].path, &(zc->key_set[i])) ) {
                    break;
                }
                zc->key_set[i].is_synced = 1;
            }

            if (zc->key_set[i].data_set[0].mode != RW) {
                break;
            }

            *ds = &(zc->key_set[i].data_set[0]);
            return 0;
        }
    }

    return -1;
}

int get_read_dataset(zk_cxt_t *zc, uint64_t key_field, uint64_t time_field, const data_set_t **ds)
{
    /* check handle. */
    if (zc == NULL || zc->zkhandle == NULL) {
        return -1;
    }

    int i, j;
    for (i=0; i<zc->key_cnt; ++i) {
        if (key_field >= zc->key_set[i].s_key && key_field < zc->key_set[i].e_key) {

            if (zc->key_set[i].is_synced == 0) {
                if (-1 == sync_keyset(zc->zkhandle, zc->key_set[i].path, &(zc->key_set[i]))) {
                    return 0;
                }
                zc->key_set[i].is_synced = 1;
            }
            for (j=0; j<zc->key_set[i].ds_cnt; ++j) {
                if (time_field >= zc->key_set[i].data_set[j].s_time
                        && time_field < zc->key_set[i].data_set[j].e_time) {
                    if (zc->key_set[i].data_set[j].mode != NA) {
                        *ds = &(zc->key_set[i].data_set[j]);
                        return 0;
                    }
                    break;
                }
            }
            break;
        }
    }

    return -1;
}


