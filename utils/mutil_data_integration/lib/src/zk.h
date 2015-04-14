/*
 * file: zk.h
 * date: 2014-08-04
 * auth: chenjianfei@daoke.me
 * desc: zk.h
 */

#pragma once

#include <string.h>
#include <stdlib.h>     // FIXME: must include, else atol convert error.
#include <stdint.h>
#include <limits.h>

#include "zookeeper_log.h"
#include "zookeeper.h"

#define KEY_SIZE        8192
#define HOST_LEN        64
#define DN_PER_DS       2   // data nodes count per data set.
#define MAX_DS_PER_KEY  16  // max dataset count per key.
#define ERR_MSG_LEN     64

// role
#define MASTER          "MASTER"
#define SLAVE           "SLAVE"
#define ERRROLE         "ERRROLE"

// cache structure.
typedef enum {
    NA = 0,     /* not available. */
    RO = 1,     /* read only. */
    RW = 2,     /* readable and writable. */
} mode_t;

typedef struct _data_node {
    char        ip[16];
    uint32_t    w_port;
    uint32_t    r_port;
    const char  *role;
} data_node_t;

typedef struct _data_set {
    char        path[NAME_MAX];         /* dataset name: DS1 */
    uint32_t    id;
    mode_t      mode;
    uint64_t    s_time;                 /* start time: 20120101000000 */
    uint64_t    e_time;                 /* end time: 20120101000000 */
    uint8_t     dn_cnt;                 /* data node count. */
    data_node_t data_node[DN_PER_DS];   /* data node array. */
} data_set_t;

typedef struct _key_set {
    char        path[NAME_MAX];             /* "/keys/0:2048" */
    uint8_t     is_synced;                  /* 0: need refresh; 1: not need refresh. */
    uint64_t    s_key;                      /* start key: 0 */
    uint64_t    e_key;                      /* end key: 2048 */
    uint8_t     ds_cnt;                     /* dataset count. */
    data_set_t  data_set[MAX_DS_PER_KEY];   /* dataset array: data_set[0] <--> read/write; data_set[>0] <--> readonly. */
} key_set_t;

typedef struct _cxt {
    char        zkhost[NAME_MAX];
    zhandle_t   *zkhandle;
    uint8_t     key_cnt;
    key_set_t   key_set[KEY_SIZE];
} zk_cxt_t;

/* apis. */
zk_cxt_t *open_zkhandler(const char *zkhost);
int close_zkhandler(zk_cxt_t *zc);

int register_zkcache(zk_cxt_t *zc);
int unregister_zkcache(zk_cxt_t *zc);

int get_write_dataset(zk_cxt_t *zc, uint64_t key_field, const data_set_t **ds);
int get_read_dataset(zk_cxt_t *zc, uint64_t key_field, uint64_t time_field, const data_set_t **ds);




