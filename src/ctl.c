#include <string.h>

#include "ctl.h"
#include "utils.h"
#include "ldb.h"
#include "logger.h"

extern struct timeval g_dbg_time;
extern short LDB_READONLY_SWITCH;

static struct _leveldb_stuff *g_ldb= NULL;

/***********Sync*************/
#define LDB_SYNCSET     1556657827
#define LDB_SYNCSET_CNT 3
#define LDB_SYNCDEL     1556647679
#define LDB_SYNCDEL_CNT 2
/************W***************/
#define LDB_SET         12291
#define LDB_SET_CNT     3
#define LDB_DEL         2143
#define LDB_DEL_MIN     2
#define LDB_MSET        223203
/************R***************/
#define LDB_GET         4179
#define LDB_GET_CNT     2
#define LDB_LRANGE      138472676
#define LDB_LRANGE_CNT  4
#define LDB_KEYS        179106
#define LDB_KEYS_CNT    2
#define LDB_INFO        149540
#define LDB_INFO_CNT    1
#define LDB_PING        269392
#define LDB_PING_CNT    1
#define LDB_EXISTS      58189240
#define LDB_EXISTS_CNT  2
#define LDB_COMPACT     789918239
#define LDB_COMPACT_CNT 1
/************O***************/
#define LDB_QUIT        294963
/***********TEST*************/
#define LDB_VALUES      249715874
#define LDB_VALUES_CNT  2
/************P***************/

static char *x_strstr(const char *s1, const char *s2, int size)
{
    const char *p = s1;
    const size_t len = strlen (s2);
    const int idx = size - len;
    for (; ((p = strchr (p, *s2)) != 0) && ((p - s1) <= idx); p++) {
        if (strncmp (p, s2, len) == 0)
            return (char *)p;
    }
    return (0);
}

int ctl_ldb_init(char *db_name)
{
    g_ldb = ldb_initialize(db_name);
    if (!g_ldb){
        perror("ldb_init");
        return -1;
    }
    return 0;
}

int ctl_ldb_close(void)
{
    log_info("close ldb.");
    ldb_close(g_ldb);
    return 0;
}

#define CHECK_BUFFER(x, y) do { \
    if (  (( offset = (p_node->svbf + p_node->gtlen - (x + y)) ) <=  0)  ||  !(x = x_strstr( (x + y), "\r\n", offset)) ) { return X_DATA_NO_ALL; } \
    else { x = x + 2; } \
} while (0)

#define PARSE_LEN(x) do { if (x == 0){ \
    if (data[ p_node->doptr ] != '$'){ \
        return X_ERROR_CMD; \
    } \
    p_new = p_old = &data[ p_node->doptr + 1 ]; \
    CHECK_BUFFER(p_new, 0); \
    x = atoi( p_old ); \
    p_node->doptr = p_new - data; } \
} while (0)
#define PARSE_MEMBER(x,y) do { if (x == 0){ \
    p_new = p_old = &data[ p_node->doptr ]; \
    CHECK_BUFFER(p_new, y); \
    x = p_old - data; \
    p_node->doptr = p_new - data; } \
} while (0)

int ctl_cmd_parse(struct data_node *p_node)
{
    int ret = 0;
    char ret_num[16] = {0};
    int loop = 0;
    int ok = 0;
    int i,j = 0;
    int offset = 0;
    size_t mlen = 0;
    char *p_new = NULL;
    char *p_old = NULL;
    char *result = NULL;
    int size = 0;

    if ( p_node->status == X_MALLOC_FAILED ){
        add_send_node(p_node, OPT_NO_MEMORY, strlen(OPT_NO_MEMORY));
        return X_MALLOC_FAILED;
    }
    if ( p_node->gtlen > MAX_CMD_LEN ){
        add_send_node(p_node, OPT_DATA_TOO_LARGE, strlen(OPT_DATA_TOO_LARGE));
        return X_DATA_TOO_LARGE;
    }
    /* parse cmd */
    char *data = p_node->svbf;
    log_info("%s", data);
    if (data[0] == '*'){
        // 1. read the arg count:
        if (p_node->kvs == 0) {
            p_new = p_old = &data[1];
            CHECK_BUFFER(p_new, 0);
            p_node->kvs = atoi( p_old );
            p_node->doptr = p_new - data;
        }
        // 2. read the request name
        if (p_node->cmd == 0){
            if (data[ p_node->doptr ] != '$'){
                goto E_OUT_PUT;
            }
            p_new = p_old = &data[ p_node->doptr + 1];
            CHECK_BUFFER(p_new, 0);
            mlen = atoi( p_old );
            p_old = p_new;
            CHECK_BUFFER(p_new, mlen);
            for (i = 0; i < mlen; i++){
                if ( *(p_old + i) > 'Z' ){
                    *(p_old + i) -= 32;/* 'A' - 'a' */
                }
                p_node->cmd = p_node->cmd * 26 + ( *(p_old + i) - 'A' );/* A~Z is 26 numbers */
            }
            p_node->doptr = p_new - data;
        }

        // 3. read a arg
        switch (p_node->cmd){
            case LDB_SET:
                if(LDB_READONLY_SWITCH == 1 || p_node->kvs != LDB_SET_CNT) {
                    goto E_OUT_PUT;
                }
                PARSE_LEN(p_node->klen);
                PARSE_MEMBER(p_node->key, p_node->klen);
                PARSE_LEN(p_node->vlen);
                PARSE_MEMBER(p_node->val, p_node->vlen);
//                x_out_time(&g_dbg_time);
                ok = ldb_put(g_ldb, &data[ p_node->key ], p_node->klen, &data[ p_node->val ], p_node->vlen);
//                x_out_time(&g_dbg_time);
                goto W_OUT_PUT;

            case LDB_DEL:
                if(LDB_READONLY_SWITCH == 1 || p_node->kvs < LDB_DEL_MIN) {
                    goto E_OUT_PUT;
                }
                loop = p_node->kvs - 1;
                for (j = 0; j < loop; ++j) {
                    PARSE_LEN(p_node->klen);
                    PARSE_MEMBER(p_node->key, p_node->klen);
                    ok = ldb_batch_delete(g_ldb, &data[ p_node->key ], p_node->klen);
                    if (ok < 0) goto E_OUT_PUT;
                    p_node->kvs -= 1;
                    p_node->key = 0;
                    p_node->klen = 0;
                }
                ok = ldb_batch_commit(g_ldb);
                if (ok < 0) goto E_OUT_PUT;
                sprintf(ret_num, OPT_NUMBER, loop);
                add_send_node(p_node, ret_num, strlen(ret_num));
                return X_DATA_IS_ALL;

            case LDB_MSET:
                /* use one thread to write, otherwise (one thread should use one leveldb_writebatch_t) or (add a mutex lock) */
                if(LDB_READONLY_SWITCH == 1){ goto E_OUT_PUT; }
                loop = (p_node->kvs - 1) / 2;
                if(loop < 1) {
                    goto E_OUT_PUT;
                }
                for (j = 0; j < loop; ++j){
                    PARSE_LEN(p_node->klen);
                    PARSE_MEMBER(p_node->key, p_node->klen);
                    PARSE_LEN(p_node->vlen);
                    PARSE_MEMBER(p_node->val, p_node->vlen);
                    ldb_batch_put(g_ldb, &data[ p_node->key ], p_node->klen, &data[ p_node->val ], p_node->vlen);
                    p_node->kvs -= 2;
                    p_node->klen = 0;
                    p_node->key = 0;
                    p_node->vlen = 0;
                    p_node->val = 0;
                }
                ok = ldb_batch_commit(g_ldb);
                goto W_OUT_PUT;

            case LDB_GET:
                if(p_node->kvs != LDB_GET_CNT) {
                    goto E_OUT_PUT;
                }
                PARSE_LEN(p_node->klen);
                PARSE_MEMBER(p_node->key, p_node->klen);
                result = ldb_get(g_ldb, &data[ p_node->key ], p_node->klen, &size);
                goto R_BULK_OUT_PUT;

            case LDB_LRANGE:
                if(p_node->kvs != LDB_LRANGE_CNT) {
                    goto E_OUT_PUT;
                }
                PARSE_LEN(p_node->klen);
                PARSE_MEMBER(p_node->key, p_node->klen);    /* look as prefix key */
                PARSE_LEN(p_node->vlen);
                PARSE_MEMBER(p_node->val, p_node->vlen);    /* look as start time */
                PARSE_LEN(p_node->vlen2);
                PARSE_MEMBER(p_node->val2, p_node->vlen2);  /* look as end time */
                log_debug("lrange prefix key = %s", &data[ p_node->key ]);
                log_debug("lrange start = %s", &data[ p_node->val ]);
                log_debug("lrange end = %s", &data[ p_node->val2 ]);
                result = ldb_lrangeget(g_ldb, &data[ p_node->key ], p_node->klen, &data[ p_node->val ],
                        p_node->vlen,  &data[ p_node->val2 ], p_node->vlen2, &size);
                goto R_MULTI_OUT_PUT;

            case LDB_KEYS:
                if(p_node->kvs != LDB_KEYS_CNT) {
                    goto E_OUT_PUT;
                }
                PARSE_LEN(p_node->klen);
                PARSE_MEMBER(p_node->key, p_node->klen);
                result = ldb_keys(g_ldb, &data[ p_node->key ], p_node->klen, &size);
                log_debug("keys: size = %d", size);
                goto R_MULTI_OUT_PUT;

            case LDB_VALUES:
                if(p_node->kvs != LDB_VALUES_CNT) {
                    goto E_OUT_PUT;
                }
                PARSE_LEN(p_node->klen);
                PARSE_MEMBER(p_node->key, p_node->klen);
                result = ldb_values(g_ldb, &data[ p_node->key ], p_node->klen, &size);
                log_debug("values: size = %d", size);
                goto R_MULTI_OUT_PUT;

            case LDB_INFO:
                if(p_node->kvs != LDB_INFO_CNT) {
                    goto E_OUT_PUT;
                }
                result = ldb_info(g_ldb, &size);
                log_debug("info: size = %d", size);
                goto R_MULTI_OUT_PUT;

            case LDB_PING:
                if(p_node->kvs != LDB_PING_CNT) {
                    goto E_OUT_PUT;
                }
                add_send_node(p_node, OPT_PONG, strlen(OPT_PONG));
                return X_DATA_IS_ALL;

            case LDB_EXISTS:
                if (p_node->kvs != LDB_EXISTS_CNT) {
                    goto E_OUT_PUT;
                }
                PARSE_LEN(p_node->klen);
                PARSE_MEMBER(p_node->key, p_node->klen);
                ok = ldb_exists(g_ldb, &(data[ p_node->key ]), p_node->klen);

                sprintf(ret_num, OPT_NUMBER, ok);
                add_send_node(p_node, ret_num, strlen(ret_num));
                return X_DATA_IS_ALL;

            case LDB_SYNCSET:
                log_debug("SYNCSET");
                if (p_node->kvs != LDB_SYNCSET_CNT) {
                    goto E_OUT_PUT;
                }
                PARSE_LEN(p_node->klen);
                PARSE_MEMBER(p_node->key, p_node->klen);
                PARSE_LEN(p_node->vlen);
                PARSE_MEMBER(p_node->val, p_node->vlen);
                ok = ldb_syncset(g_ldb, &data[ p_node->key ], p_node->klen, &data[ p_node->val ], p_node->vlen);
                goto W_OUT_PUT;

            case LDB_SYNCDEL:
                log_debug("SYNCDEL");
                if (p_node->kvs != LDB_SYNCDEL_CNT) {
                    goto E_OUT_PUT;
                }
                PARSE_LEN(p_node->klen);
                PARSE_MEMBER(p_node->key, p_node->klen);
                ok = ldb_syncdel(g_ldb, &data[ p_node->key ], p_node->klen);
                goto W_OUT_PUT;

            case LDB_COMPACT:
                log_debug("COMPACT");
                if(p_node->kvs != LDB_COMPACT_CNT) {
                    goto E_OUT_PUT;
                }
                ldb_compact(g_ldb);
                add_send_node(p_node, OPT_OK, strlen(OPT_OK));
                return X_DATA_IS_ALL;

            case LDB_QUIT:
                return X_CLIENT_QUIT;

            default:
                goto E_OUT_PUT;
        }
E_OUT_PUT:
        add_send_node(p_node, OPT_CMD_ERROR, strlen(OPT_CMD_ERROR));
        return X_ERROR_CMD;

W_OUT_PUT:
        if (ok == 0 || ok == 1){
            add_send_node(p_node, OPT_OK, strlen(OPT_OK));
            return X_DATA_IS_ALL; 
        }else{
            add_send_node(p_node, OPT_LDB_ERROR, strlen(OPT_LDB_ERROR));
            return X_ERROR_LDB; 
        }

R_BULK_OUT_PUT:
        if (result){
            char tmp[32] = {0};
            sprintf(tmp, "$%d\r\n", size);
            ret = add_send_node(p_node, tmp, strlen(tmp));
            ret = add_send_node(p_node, result, size);
            leveldb_free(result);
            if ( ret == X_MALLOC_FAILED ){
                clean_send_node(p_node);
                add_send_node(p_node, OPT_NO_MEMORY, strlen(OPT_NO_MEMORY));
                return X_MALLOC_FAILED;
            }
            ret = add_send_node(p_node, "\r\n", 2);
            return X_DATA_IS_ALL; 
        }else{
            if (size == 0){
                add_send_node(p_node, OPT_NULL, strlen(OPT_NULL));
                return X_DATA_IS_ALL; 
            }
            else{
                add_send_node(p_node, OPT_LDB_ERROR, strlen(OPT_LDB_ERROR));
                return X_ERROR_LDB;
            }
        }
R_MULTI_OUT_PUT:
        if (result){
            set_send_node(p_node, result, size);
            return X_DATA_IS_ALL; 
        }else{
            if (size == 0){
                add_send_node(p_node, OPT_EMPTY_MULTI, strlen(OPT_EMPTY_MULTI));
                return X_DATA_IS_ALL; 
            }
            else{
                add_send_node(p_node, OPT_LDB_ERROR, strlen(OPT_LDB_ERROR));
                return X_ERROR_LDB;
            }
        }
    } else if (data[0] == '$'){
#if 0
        // 1. read the request name
        if (p_node->cmd == 0){
            p_new = p_old = &data[1];
            CHECK_BUFFER(p_new, 0);
            mlen = atoi( p_old );
            p_old = p_new;
            CHECK_BUFFER(p_new, mlen);
            for (i = 0; i < mlen; i++){
                p_node->cmd = p_node->cmd * 26 + ( *(p_old + i) - 'A' );/* A~Z is 26 numbers */
            }
            p_node->doptr = p_new - data;
        }
        log_debug("%d\n", p_node->cmd);
        // 2. read a arg
        switch (p_node->cmd){
            case LDB_QUIT:
                return CLIENT_QUIT;
            default:
                log_debug("-----------------------------------------------------\n");
                return -1;
        }

        //TODO
#endif
        add_send_node(p_node, OPT_CMD_ERROR, strlen(OPT_CMD_ERROR));
        return X_ERROR_CMD;
    }else{
        add_send_node(p_node, OPT_CMD_ERROR, strlen(OPT_CMD_ERROR));
        return X_ERROR_CMD;
    }
    return X_DATA_IS_ALL; 
}


int ctl_cmd_done(struct data_node *p_node)
{
    p_node->kvs = 0;
    p_node->cmd = 0;
    p_node->klen = 0;
    p_node->vlen = 0;
    p_node->vlen2 = 0;

    p_node->doptr = 0;
    p_node->key = 0;
    p_node->val = 0;
    p_node->val2 = 0;
    return 0;
}
int ctl_status_clean(struct data_node *p_node)
{
    p_node->status = X_DONE_OK;
    return 0;
}
