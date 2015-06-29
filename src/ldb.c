/*
 * file: ldb.c
 * auth: cjfeii@126.com
 * date: Aug 8, 2014
 * desc: level db acess.
 */

#include <string.h>
#include <unistd.h>
#include <ftw.h>
#include <sys/utsname.h>
#include <time.h>

#include "logger.h"
#include "ldb.h"
#include "ctl.h"
#include "version.h"
#include "read_cfg.h"
#include "binlog.h"
#include "slave.h"

extern size_t LDB_WRITE_BUFFER_SIZE;
extern size_t LDB_BLOCK_SIZE;
extern size_t LDB_CACHE_LRU_SIZE;
extern short LDB_BLOOM_KEY_SIZE;
extern char *LDB_WORK_PATH;
extern short LDB_READONLY_SWITCH;
extern int W_PORT;
extern int R_PORT;
extern time_t LDB_START_TIME;
//extern enum node_type g_node_type;
extern struct cfg_info g_cfg_pool;

struct _leveldb_stuff *ldb_initialize(char *path)
{

    struct _leveldb_stuff *ldbs = NULL;
    leveldb_cache_t *cache;
    leveldb_filterpolicy_t *policy;
    char* err = NULL;

    ldbs = malloc(sizeof(struct _leveldb_stuff));
    memset(ldbs, 0, sizeof(struct _leveldb_stuff));

    ldbs->options = leveldb_options_create();

    snprintf(ldbs->dbname, sizeof(ldbs->dbname), "%s/%s", LDB_WORK_PATH, path);

    cache = leveldb_cache_create_lru(LDB_CACHE_LRU_SIZE);
    policy = leveldb_filterpolicy_create_bloom(LDB_BLOOM_KEY_SIZE);

    leveldb_options_set_filter_policy(ldbs->options, policy);
    leveldb_options_set_cache(ldbs->options, cache);
    leveldb_options_set_block_size(ldbs->options, LDB_BLOCK_SIZE);
    leveldb_options_set_write_buffer_size(ldbs->options, LDB_WRITE_BUFFER_SIZE);
    leveldb_options_set_compaction_speed(ldbs->options, g_cfg_pool.ldb_compaction_speed);
#if defined(OPEN_COMPRESSION)
    leveldb_options_set_compression(ldbs->options, leveldb_snappy_compression);
#else
    leveldb_options_set_compression(ldbs->options, leveldb_no_compression);
#endif
    /* R */
    ldbs->roptions = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(ldbs->roptions, 1);
    leveldb_readoptions_set_fill_cache(ldbs->roptions, 1);/* set 1 is faster */

    /* W */
    ldbs->woptions = leveldb_writeoptions_create();
#ifdef SYNC_PUT
    leveldb_writeoptions_set_sync(ldbs->woptions, 1);
#else
    leveldb_writeoptions_set_sync(ldbs->woptions, 0);
#endif

    /* B */
    ldbs->wbatch = leveldb_writebatch_create();

    leveldb_options_set_create_if_missing(ldbs->options, 1);
    ldbs->db = leveldb_open(ldbs->options, ldbs->dbname, &err);

    if (err) {
        fprintf(stderr, "%s", err);
        leveldb_free(err);
        err = NULL;
        free(ldbs);
        return NULL ;
    }

    /* initial binlog. */
    binlog_open(ldbs, g_cfg_pool.has_slave);

    /* initial slave. */
    if (slave_open(ldbs, g_cfg_pool.has_slave, g_cfg_pool.slave_ip, g_cfg_pool.slave_wport) != 0) {
        return NULL;
    }

    return ldbs;
}

/*
 * Close the ldb.
 */
void ldb_close(struct _leveldb_stuff *ldbs)
{
    leveldb_close(ldbs->db);

    leveldb_options_destroy(ldbs->options);
    leveldb_readoptions_destroy(ldbs->roptions);
    leveldb_writeoptions_destroy(ldbs->woptions);
    leveldb_writebatch_destroy(ldbs->wbatch);
}

/*
 * Destroy the ldb.
 */
void ldb_destroy(struct _leveldb_stuff *ldbs)
{
    char* err = NULL;
    leveldb_close(ldbs->db);
    leveldb_destroy_db(ldbs->options, ldbs->dbname, &err);
    if (err) {
        fprintf(stderr, "%s", err);
        leveldb_free(err);
        err = NULL;
    }
    leveldb_options_destroy(ldbs->options);
    leveldb_readoptions_destroy(ldbs->roptions);
    leveldb_writeoptions_destroy(ldbs->woptions);
    leveldb_writebatch_destroy(ldbs->wbatch);
    free(ldbs);
}

char *ldb_get(struct _leveldb_stuff *ldbs, const char *key, size_t klen, int *vlen)
{
    char *err = NULL;
    char *val = NULL;
    val = leveldb_get(ldbs->db, ldbs->roptions, key, klen, (size_t *) vlen, &err);
    if (err) {
        fprintf(stderr, "\n%s\n", err);
        leveldb_free(err);
        err = NULL;
        *vlen = -1;
        return NULL ;
    } else {
        return val;
    }
}

//static int get_number_len(int len)
int get_number_len(size_t len)
{
    int i = 1;
    size_t mlen = len;
    while ((mlen /= 10) >= 1) {
        i++;
    }
    return i;
}


char *set_bulk(char *dst, const char *put, int len)
{
    char *ptr = NULL;
    sprintf(dst, "$%d\r\n", len);
    int hlen = strlen(dst);
    ptr = dst + hlen;
    memcpy(ptr, put, len);
    ptr += len;
    memcpy(ptr, "\r\n", 2);
    return (ptr + 2);
}

char *ldb_tsget(struct _leveldb_stuff *ldbs, const char *st_key, size_t st_klen, const char *ed_key, size_t ed_klen, int *size)
{
    char *err = NULL;
    char *result = NULL;
    char *p_dst = NULL;
    char *p_val = NULL;
    char *p_key = NULL;
    size_t klen = 0;
    size_t vlen = 0;
    int index = 0;
    int i, j, z = 0;
    struct kv_list list = { 0 };
    struct some_kv *p_new, *p_old, *p_tmp = NULL;

    leveldb_iterator_t* iter = leveldb_create_iterator(ldbs->db, ldbs->roptions);
    leveldb_iterator_t* iter_save = iter;
    if (!!leveldb_iter_valid(iter)) {/* first use it is invalid */
        fprintf(stderr, "%s:%d: this iter is valid already!\n", __FILE__, __LINE__);
        *size = -1;
        return NULL ;
    }
    leveldb_iter_seek(iter, st_key, st_klen);
    p_key = (char *) st_key;
    p_old = p_new = &list.head;
    /* while ( leveldb_iter_valid(iter) && (0 >= strncmp( ed_key, p_key, ed_klen )) ); */
    if (0 <= strncmp(ed_key, p_key, ed_klen)) {
        while (leveldb_iter_valid(iter)) {
            /* parse kv */
            p_key = (char *) leveldb_iter_key(iter, &klen);
            log_debug("%p iter key = %s, klen = %ld\n", p_key, p_key, klen);

            p_val = (char *) leveldb_iter_value(iter, &vlen);
            log_debug("%p iter key = %s, klen = %ld\n", p_val, p_val, vlen);

            leveldb_iter_get_error(iter, &err);
            if (err) {
                goto FAIL_ITER_PARSE;
            }
            if (0 > strncmp(ed_key, p_key, ed_klen)) {
                log_debug("--------------break------------------\n");
                break;
            }
            /* save parse */
            list.count++;/* kv counts */
            list.klens += klen;
            list.knubs += get_number_len(klen);
            list.vlens += vlen;
            list.vnubs += get_number_len(vlen);
            index = list.count % SOME_KV_NODES_COUNT;
            if ((list.count / SOME_KV_NODES_COUNT >= 1) && (index == 1)) {
                /* new store */
                p_new = malloc(sizeof(struct some_kv));
                if (p_new == NULL ) {
                    /* free stroe */
                    index = GET_NEED_COUNT( list.count, SOME_KV_NODES_COUNT );
                    p_tmp = &list.head;
                    for (i = 0, z = list.count - 1; (i < index) && (p_tmp != NULL ); i++ ) {
                        for (j = 0; (j < SOME_KV_NODES_COUNT) && (z > 0); j++, z--) {
                            free(p_tmp->nodes[j].key);
                            free(p_tmp->nodes[j].val);
                        }
                        p_old = p_tmp;
                        p_tmp = p_tmp->next;
                        if (p_old != &list.head) {
                            free(p_old);
                        }
                    }
                    goto FAIL_MEMORY;
                }
                memset(p_new, 0, sizeof(struct some_kv));
                p_old->next = p_new;
                p_new->prev = p_old;
                p_old = p_new;
            }

            /*
             * fix bug: index is error if list.count = n * SOME_KV_NODES_COUNT(1024),
             *          SOME_KV_NODES_COUNT = 1024, n > 0.
             */
            if (index == 0) {
                index = SOME_KV_NODES_COUNT;
            }

            /* save key */
            p_new->nodes[index - 1].klen = klen;
            p_new->nodes[index - 1].key = malloc(GET_NEED_COUNT( klen, G_PAGE_SIZE ) * G_PAGE_SIZE);
            memcpy(p_new->nodes[index - 1].key, p_key, klen);

            /* save val */
            p_new->nodes[index - 1].vlen = vlen;
            p_new->nodes[index - 1].val = malloc(GET_NEED_COUNT( vlen, G_PAGE_SIZE ) * G_PAGE_SIZE);
            memcpy(p_new->nodes[index - 1].val, p_val, vlen);

            /* find next */
            leveldb_iter_next(iter);
        }
    }
    /* create result */
    if (list.count > 0) {
        /* has members */
        /* *2\r\n$5\r\nmykey\r\n$5\r\nmyval\r\n */
        *size = strlen("*\r\n") + get_number_len(list.count * 2)
            + strlen("$\r\n\r\n") * (list.count * 2) + list.knubs + list.klens + list.vnubs
            + list.vlens;
        index = GET_NEED_COUNT( *size, G_PAGE_SIZE ) * G_PAGE_SIZE;
        result = (char *) malloc(index);
        if (result == NULL ) goto FAIL_MEMORY;
        memset(result, 0, index);
        log_debug("----->>>ALL SIZE IS %d, BUFF %p : LEN IS %d\n", *size, result, index);

        /* split piece */
        index = GET_NEED_COUNT( list.count, SOME_KV_NODES_COUNT );
        p_tmp = &list.head;
        sprintf(result, "*%d\r\n", list.count * 2);
        p_dst = result + strlen(result);
        for (i = 0, z = list.count; (i < index) && (p_tmp != NULL ); i++ ) {
            for (j = 0; (j < SOME_KV_NODES_COUNT) && (z > 0); j++, z--) {
                p_dst = set_bulk(p_dst, p_tmp->nodes[j].key, p_tmp->nodes[j].klen);
                free(p_tmp->nodes[j].key);
                p_dst = set_bulk(p_dst, p_tmp->nodes[j].val, p_tmp->nodes[j].vlen);
                free(p_tmp->nodes[j].val);
            }
            p_old = p_tmp;
            p_tmp = p_tmp->next;
            if (p_old != &list.head) {
                free(p_old);
            }
        }
    } else {
        /* no members */
        *size = 0;
    }
    leveldb_iter_destroy(iter_save);
    return result;
FAIL_ITER_PARSE: fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, (err));
                 leveldb_free(err);
                 err = NULL;
                 leveldb_iter_destroy(iter);
                 *size = -1;
                 return NULL ;
FAIL_MEMORY: fprintf(stderr, "%s:%d: FAILED MALLOC !\n", __FILE__, __LINE__);
             leveldb_iter_destroy(iter);
             *size = -1;
             return NULL ;
}

/*
 * for lrange key ts1 ts2
 */
char *ldb_lrangeget(struct _leveldb_stuff *ldbs, const char *pre_key, size_t pre_klen, const char *st_time, size_t st_tlen, const char *ed_time, size_t ed_tlen, int *size)
{
    char *err = NULL;
    char *result = NULL;
    char *p_dst = NULL;
    char *p_val = NULL;
    char *p_key = NULL;
    size_t klen = 0;
    size_t vlen = 0;
    int index = 0;
    int i, j, z = 0;
    struct kv_list list = { 0 };
    struct some_kv *p_new, *p_old, *p_tmp = NULL;

    /* piece the start_key && end_key together */
    size_t st_klen = pre_klen + st_tlen;
    size_t ed_klen = pre_klen + ed_tlen;
    char *st_key = malloc(st_klen);
    char *ed_key = malloc(ed_klen);
    memcpy(st_key, pre_key, pre_klen);
    memcpy(st_key + pre_klen, st_time, st_tlen);
    memcpy(ed_key, pre_key, pre_klen);
    memcpy(ed_key + pre_klen, ed_time, ed_tlen);

    leveldb_iterator_t* iter = leveldb_create_iterator(ldbs->db, ldbs->roptions);
    leveldb_iterator_t* iter_save = iter;
    if (!!leveldb_iter_valid(iter)) {/* first use it is invalid */
        fprintf(stderr, "%s:%d: this iter is valid already!\n", __FILE__, __LINE__);
        *size = -1;
        return NULL ;
    }
    leveldb_iter_seek(iter, st_key, st_klen);
    p_key = (char *) st_key;
    p_old = p_new = &list.head;
    /* while ( leveldb_iter_valid(iter) && (0 >= strncmp( ed_key, p_key, ed_klen )) ); */
    if (0 <= strncmp(ed_key, p_key, ed_klen)) {
        while (leveldb_iter_valid(iter)) {
            /* parse kv */
            p_key = (char *) leveldb_iter_key(iter, &klen);
            log_debug("%p iter key = %s, klen = %ld\n", p_key, p_key, klen);

            p_val = (char *) leveldb_iter_value(iter, &vlen);
            // log_debug("%p iter key = %s, klen = %ld\n", p_val, p_val, vlen);

            leveldb_iter_get_error(iter, &err);
            if (err) {
                goto FAIL_ITER_PARSE;
            }

            // if (0 > strncmp( ed_key, p_key, ed_klen ) || list.count >= LRANGE_MAX){
            if (0 > strncmp(ed_key, p_key, ed_klen)) {
                log_debug("--------------break------------------\n");
                break;
            }
            /* save parse */
            list.count++;/* kv counts */
            list.klens += klen;
            list.knubs += get_number_len(klen);
            list.vlens += vlen;
            list.vnubs += get_number_len(vlen);
            index = list.count % SOME_KV_NODES_COUNT;
            if ((list.count / SOME_KV_NODES_COUNT >= 1) && (index == 1)) {
                /* new store */
                p_new = malloc(sizeof(struct some_kv));
                if (p_new == NULL ) {
                    /* free stroe */
                    index = GET_NEED_COUNT( list.count, SOME_KV_NODES_COUNT );
                    p_tmp = &list.head;
                    for (i = 0, z = list.count - 1; (i < index) && (p_tmp != NULL ); i++ ) {
                        for (j = 0; (j < SOME_KV_NODES_COUNT) && (z > 0); j++, z--) {
                            free(p_tmp->nodes[j].key);
                            free(p_tmp->nodes[j].val);
                        }
                        p_old = p_tmp;
                        p_tmp = p_tmp->next;
                        if (p_old != &list.head) {
                            free(p_old);
                        }
                    }
                    goto FAIL_MEMORY;
                }
                memset(p_new, 0, sizeof(struct some_kv));
                p_old->next = p_new;
                p_new->prev = p_old;
                p_old = p_new;
            }

            /*
             * fix bug: index is error if list.count = n * SOME_KV_NODES_COUNT(1024),
             *          SOME_KV_NODES_COUNT = 1024, n > 0.
             */
            if (index == 0) {
                index = SOME_KV_NODES_COUNT;
            }

            /* save key */
            p_new->nodes[index - 1].klen = klen;
            p_new->nodes[index - 1].key = malloc(GET_NEED_COUNT( klen, G_PAGE_SIZE ) * G_PAGE_SIZE);
            memcpy(p_new->nodes[index - 1].key, p_key, klen);

            /* save val */
            p_new->nodes[index - 1].vlen = vlen;
            p_new->nodes[index - 1].val = malloc(GET_NEED_COUNT( vlen, G_PAGE_SIZE ) * G_PAGE_SIZE);
            memcpy(p_new->nodes[index - 1].val, p_val, vlen);

            /* find next */
            leveldb_iter_next(iter);
        }
        }

        /* free space */
        free(st_key);
        free(ed_key);

        /* create result */
        if (list.count > 0) {
            /* has members */
            /* *2\r\n$5\r\nmykey\r\n$5\r\nmyval\r\n */
            *size = strlen("*\r\n") + get_number_len(list.count * 2)
                + strlen("$\r\n\r\n") * (list.count * 2) + list.knubs + list.klens + list.vnubs
                + list.vlens;
            index = GET_NEED_COUNT( *size, G_PAGE_SIZE ) * G_PAGE_SIZE;
            result = (char *) malloc(index);
            if (result == NULL ) goto FAIL_MEMORY;
            memset(result, 0, index);
            log_debug("----->>>ALL SIZE IS %d, BUFF %p : LEN IS %d\n", *size, result, index);

            /* split piece */
            index = GET_NEED_COUNT( list.count, SOME_KV_NODES_COUNT );
            p_tmp = &list.head;
            sprintf(result, "*%d\r\n", list.count * 2);
            p_dst = result + strlen(result);
            for (i = 0, z = list.count; (i < index) && (p_tmp != NULL ); i++ ) {
                for (j = 0; (j < SOME_KV_NODES_COUNT) && (z > 0); j++, z--) {
                    p_dst = set_bulk(p_dst, p_tmp->nodes[j].key, p_tmp->nodes[j].klen);
                    free(p_tmp->nodes[j].key);
                    p_dst = set_bulk(p_dst, p_tmp->nodes[j].val, p_tmp->nodes[j].vlen);
                    free(p_tmp->nodes[j].val);
                }
                p_old = p_tmp;
                p_tmp = p_tmp->next;
                if (p_old != &list.head) {
                    free(p_old);
                }
            }
        } else {
            /* no members */
            *size = 0;
        }
        leveldb_iter_destroy(iter_save);
        return result;
FAIL_ITER_PARSE: fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, (err));
                 leveldb_free(err);
                 err = NULL;
                 leveldb_iter_destroy(iter);
                 *size = -1;
                 return NULL ;
FAIL_MEMORY: fprintf(stderr, "%s:%d: FAILED MALLOC !\n", __FILE__, __LINE__);
             leveldb_iter_destroy(iter);
             *size = -1;
             return NULL ;
    }

    char *ldb_keys(struct _leveldb_stuff *ldbs, const char *ptn, size_t ptn_len, int *size)
    {
        char *err = NULL;
        char *result = NULL;
        char *p_dst = NULL;
        char *p_key = NULL;
        size_t klen = 0;
        int index = 0;
        int i, j, z = 0;
        struct kv_list list = { 0 };
        struct some_kv *p_new, *p_old, *p_tmp = NULL;

        leveldb_iterator_t* iter = leveldb_create_iterator(ldbs->db, ldbs->roptions);
        leveldb_iterator_t* iter_save = iter;
        if (!!leveldb_iter_valid(iter)) {/* first use it is invalid */
            fprintf(stderr, "%s:%d: this iter is valid already!\n", __FILE__, __LINE__);
            *size = -1;
            return NULL ;
        }

        leveldb_iter_seek(iter, ptn, ptn_len);
        p_old = p_new = &list.head;
        while (leveldb_iter_valid(iter)) {
            /* parse kv */
            p_key = (char *) leveldb_iter_key(iter, &klen);
            log_debug("%p iter key = %s, klen = %ld\n", p_key, p_key, klen);

            leveldb_iter_get_error(iter, &err);
            if (err) {
                goto FAIL_ITER_PARSE;
            }

            if (!prefix_match_len(ptn, ptn_len, p_key, klen)) {
                break;
            }

            /* save parse */
            list.count++;/* kv counts */
            list.klens += klen;
            list.knubs += get_number_len(klen);
            index = list.count % SOME_KV_NODES_COUNT;
            if ((list.count / SOME_KV_NODES_COUNT >= 1) && (index == 1)) {
                /* new store */
                p_new = malloc(sizeof(struct some_kv));
                if (p_new == NULL ) {
                    /* free store */
                    index = GET_NEED_COUNT( list.count, SOME_KV_NODES_COUNT );
                    p_tmp = &list.head;
                    for (i = 0, z = list.count - 1; (i < index) && (p_tmp != NULL ); i++ ) {
                        for (j = 0; (j < SOME_KV_NODES_COUNT) && (z > 0); j++, z--) {
                            free(p_tmp->nodes[j].key);
                        }
                        p_old = p_tmp;
                        p_tmp = p_tmp->next;
                        if (p_old != &list.head) {
                            free(p_old);
                        }
                    }
                    goto FAIL_MEMORY;
                }
                memset(p_new, 0, sizeof(struct some_kv));
                p_old->next = p_new;
                p_new->prev = p_old;
                p_old = p_new;
            }

            /*
             * fix bug: index is error if list.count = n * SOME_KV_NODES_COUNT(1024),
             *          SOME_KV_NODES_COUNT = 1024, n > 0.
             */
            if (index == 0) {
                index = SOME_KV_NODES_COUNT;
            }

            /* save key */
            p_new->nodes[index - 1].klen = klen;
            p_new->nodes[index - 1].key = malloc(GET_NEED_COUNT( klen, G_PAGE_SIZE ) * G_PAGE_SIZE);
            memcpy(p_new->nodes[index - 1].key, p_key, klen);

            /* find next */
            leveldb_iter_next(iter);

        }

        /* create result */
        if (list.count > 0) {
            /* has members */
            /* *2\r\n$5\r\nmykey\r\n$5\r\nmyval\r\n */
            *size = strlen("*\r\n") + get_number_len(list.count) + strlen("$\r\n\r\n") * (list.count)
                + list.knubs + list.klens;
            index = GET_NEED_COUNT( *size, G_PAGE_SIZE ) * G_PAGE_SIZE;
            result = (char *) malloc(index);
            if (result == NULL ) goto FAIL_MEMORY;
            memset(result, 0, index);
            log_debug("----->>>ALL SIZE IS %d, BUFF %p : LEN IS %d\n", *size, result, index);

            /* split piece */
            index = GET_NEED_COUNT( list.count, SOME_KV_NODES_COUNT );
            p_tmp = &list.head;
            sprintf(result, "*%d\r\n", list.count);
            p_dst = result + strlen(result);
            for (i = 0, z = list.count; (i < index) && (p_tmp != NULL ); i++ ) {
                for (j = 0; (j < SOME_KV_NODES_COUNT) && (z > 0); j++, z--) {
                    p_dst = set_bulk(p_dst, p_tmp->nodes[j].key, p_tmp->nodes[j].klen);
                    free(p_tmp->nodes[j].key);
                }
                p_old = p_tmp;
                p_tmp = p_tmp->next;
                if (p_old != &list.head) {
                    free(p_old);
                }
            }
        } else {
            /* no members */
            *size = 0;
        }

        leveldb_iter_destroy(iter_save);
        return result;
FAIL_ITER_PARSE: fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, (err));
                 leveldb_free(err);
                 err = NULL;
                 leveldb_iter_destroy(iter);
                 *size = -1;
                 return NULL ;
FAIL_MEMORY: fprintf(stderr, "%s:%d: FAILED MALLOC !\n", __FILE__, __LINE__);
             leveldb_iter_destroy(iter);
             *size = -1;
             return NULL ;
    }

    char *ldb_values(struct _leveldb_stuff *ldbs, const char *ptn, size_t ptn_len, int *size)
    {
        char *err = NULL;
        char *result = NULL;
        char *p_dst = NULL;
        char *p_key = NULL;
        char *p_value = NULL;
        size_t klen = 0;
        size_t vlen = 0;
        int index = 0;
        int i, j, z = 0;
        struct kv_list list = { 0 };
        struct some_kv *p_new, *p_old, *p_tmp = NULL;

        leveldb_iterator_t* iter = leveldb_create_iterator(ldbs->db, ldbs->roptions);
        leveldb_iterator_t* iter_save = iter;
        if (!!leveldb_iter_valid(iter)) {/* first use it is invalid */
            fprintf(stderr, "%s:%d: this iter is valid already!\n", __FILE__, __LINE__);
            *size = -1;
            return NULL ;
        }

        leveldb_iter_seek_to_first(iter);
        p_old = p_new = &list.head;
        while (leveldb_iter_valid(iter)) {
            /* parse kv */
            p_key = (char *) leveldb_iter_key(iter, &klen);
            log_debug("%p iter key = %s, klen = %ld\n", p_key, p_key, klen);
            p_value = (char *) leveldb_iter_value(iter, &vlen);

            leveldb_iter_get_error(iter, &err);
            if (err) {
                goto FAIL_ITER_PARSE;
            }

            if (!string_match_len(ptn, ptn_len, p_value, vlen, 0)) {
                leveldb_iter_next(iter);
                continue;
            }

            /* save parse */
            list.count++;/* kv counts */
            list.klens += klen;
            list.knubs += get_number_len(klen);
            index = list.count % SOME_KV_NODES_COUNT;
            if ((list.count / SOME_KV_NODES_COUNT >= 1) && (index == 1)) {
                /* new store */
                p_new = malloc(sizeof(struct some_kv));
                if (p_new == NULL ) {
                    /* free store */
                    index = GET_NEED_COUNT( list.count, SOME_KV_NODES_COUNT );
                    p_tmp = &list.head;
                    for (i = 0, z = list.count - 1; (i < index) && (p_tmp != NULL ); i++ ) {
                        for (j = 0; (j < SOME_KV_NODES_COUNT) && (z > 0); j++, z--) {
                            free(p_tmp->nodes[j].key);
                        }
                        p_old = p_tmp;
                        p_tmp = p_tmp->next;
                        if (p_old != &list.head) {
                            free(p_old);
                        }
                    }
                    goto FAIL_MEMORY;
                }
                memset(p_new, 0, sizeof(struct some_kv));
                p_old->next = p_new;
                p_new->prev = p_old;
                p_old = p_new;
            }

            /*
             * fix bug: index is error if list.count = n * SOME_KV_NODES_COUNT(1024),
             *          SOME_KV_NODES_COUNT = 1024, n > 0.
             */
            if (index == 0) {
                index = SOME_KV_NODES_COUNT;
            }

            /* save key */
            p_new->nodes[index - 1].klen = klen;
            p_new->nodes[index - 1].key = malloc(GET_NEED_COUNT( klen, G_PAGE_SIZE ) * G_PAGE_SIZE);
            memcpy(p_new->nodes[index - 1].key, p_key, klen);

            /* find next */
            leveldb_iter_next(iter);
        }

        /* create result */
        if (list.count > 0) {
            /* has members */
            /* *2\r\n$5\r\nmykey\r\n$5\r\nmyval\r\n */
            *size = strlen("*\r\n") + get_number_len(list.count) + strlen("$\r\n\r\n") * (list.count)
                + list.knubs + list.klens;
            index = GET_NEED_COUNT( *size, G_PAGE_SIZE ) * G_PAGE_SIZE;
            result = (char *) malloc(index);
            if (result == NULL ) goto FAIL_MEMORY;
            memset(result, 0, index);
            log_debug("----->>>ALL SIZE IS %d, BUFF %p : LEN IS %d\n", *size, result, index);

            /* split piece */
            index = GET_NEED_COUNT( list.count, SOME_KV_NODES_COUNT );
            p_tmp = &list.head;
            sprintf(result, "*%d\r\n", list.count);
            p_dst = result + strlen(result);
            for (i = 0, z = list.count; (i < index) && (p_tmp != NULL ); i++ ) {
                for (j = 0; (j < SOME_KV_NODES_COUNT) && (z > 0); j++, z--) {
                    p_dst = set_bulk(p_dst, p_tmp->nodes[j].key, p_tmp->nodes[j].klen);
                    free(p_tmp->nodes[j].key);
                }
                p_old = p_tmp;
                p_tmp = p_tmp->next;
                if (p_old != &list.head) {
                    free(p_old);
                }
            }
        } else {
            /* no members */
            *size = 0;
        }

        leveldb_iter_destroy(iter_save);
        return result;
FAIL_ITER_PARSE: fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, (err));
                 leveldb_free(err);
                 err = NULL;
                 leveldb_iter_destroy(iter);
                 *size = -1;
                 return NULL ;
FAIL_MEMORY: fprintf(stderr, "%s:%d: FAILED MALLOC !\n", __FILE__, __LINE__);
             leveldb_iter_destroy(iter);
             *size = -1;
             return NULL ;
    }

    static unsigned long dbsize = 0;
    static int sum(const char *fpath, const struct stat *sb, int typeflag)
    {
        dbsize += sb->st_size;
        return 0;
    }

#define INFO_MAX 2048

    char *ldb_info(struct _leveldb_stuff *ldbs, int *size)
    {
        char *result = (char *)calloc(1, INFO_MAX);

        /* #Server: */
        strcpy(result, "+\n#Server:\n");
        {
            /* version */
            strcat(result, TSDB_VERSION"\n");

            /* node type */
            switch (g_cfg_pool.node_type) {
                case SINGLE:
                    strcat(result, "type:SINGLE\n");
                    break;
                case CLUSTER:
                    strcat(result, "type:CLUSTER\n");
                    break;
            }

            /* tsdb mode */
            if (LDB_READONLY_SWITCH == 1) {
                strcat(result, "mode:READONLY\n");
            } else {
                strcat(result, "mode:READ WRITE\n");
            }

            /* os */
            struct utsname name;
            uname(&name);
            sprintf(result + strlen(result), "os:%s %s %s\n", name.sysname, name.release, name.machine);

            /* pid */
            sprintf(result + strlen(result), "process_id:%ld\n",(long) getpid());

            /* write_port */
            sprintf(result + strlen(result), "write_port:%d\n",W_PORT);

            /* read_port */
            sprintf(result + strlen(result), "read_port:%d\n",R_PORT);

            /* uptime_in_seconds */
            time_t curtime;
            time(&curtime);
            sprintf(result + strlen(result), "uptime_in_seconds:%ld\n", curtime - LDB_START_TIME);
            /* uptime_in_days */
            sprintf(result + strlen(result), "uptime_in_days:%ld\n", (curtime - LDB_START_TIME) / (24 * 3600));

            /* db_size */
            log_debug("%s", ldbs->dbname);
            if (!ldbs->dbname || access(ldbs->dbname, R_OK)) {
                log_error("%s", "failed to get database size!");
                return NULL ;
            }
            dbsize = 0;
            if (ftw(ldbs->dbname, &sum, 1)) {
                log_error("%s", "failed to get database size!");
                return NULL ;
            }
            sprintf(result + strlen(result), "db_size_in_bytes:%ld\n", dbsize);

            if (dbsize >= 1073741824) { // 1024*1024*1024: GB
                sprintf(result + strlen(result), "db_size_in_human:%.2fG\n", dbsize*1.0 / (1073741824));
            } else if (dbsize >= 1048576) { // 1024*1024: MB
                sprintf(result + strlen(result), "db_size_in_human:%.2fM\n", dbsize*1.0 / (1048576));
            } else if (1048576 >= 1024) { // 1024: KB
                sprintf(result + strlen(result), "db_size_in_human:%.2fK\n", dbsize*1.0 / (1024));
            } else {
                sprintf(result + strlen(result), "db_size_in_human:%ldB\n", dbsize);
            }
        }

        /* # Keyspace. */
        strcat(result, "\n#Keyspace:\n");
        {
            /* key_set. */
            sprintf(result + strlen(result), "key_set:[%ld, %ld]\n", g_cfg_pool.key_start, g_cfg_pool.key_end);

            /* time range. */
            sprintf(result + strlen(result), "time_range:[%ld ,%ld)\n", g_cfg_pool.start_time, g_cfg_pool.end_time);
        }

        /* # Replication. */
        strcat(result, "\n#Replication:\n");
        {
            switch (g_cfg_pool.role) {
                case MASTER:
                    strcat(result, "role:MASTER\n");
                    break;
                case SLAVE:
                    strcat(result, "role:SLAVE\n");
                    break;
                default:
                    strcat(result, "role:SINGLE\n");
                    break;
            }

            if (g_cfg_pool.has_slave == 0) {
                strcat(result, "has_slave:NO\n");
            } else {
                strcat(result, "has_slave:YES\n");
                sprintf(result + strlen(result), "slave_ip:%s\n", g_cfg_pool.slave_ip);
                sprintf(result + strlen(result), "slave_port:%d\n", g_cfg_pool.slave_wport);
                sprintf(result + strlen(result), "is_connect:%s\n", "TODO");
            }
        }

        /* #LeveldbStatus: */
        strcat(result, "\n#Leveldb status:\n");
        {
            /* get property. */
            strcat(result, leveldb_property_value(ldbs->db, "leveldb.stats"));
        }

        strcat(result, "\r\n");
        *size = strlen(result);
        return result;
    }

    inline int ldb_put(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen)
    {
        return binlog_put(ldbs, key, klen, value, vlen);
    }

    // TODO: leveldb_writebatch_delete
    inline int ldb_delete(struct _leveldb_stuff *ldbs, const char *key, size_t klen)
    {
        return binlog_delete(ldbs, key, klen);
    }

    int ldb_batch_put(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen)
    {
        return binlog_batch_put(ldbs, key, klen, value, vlen);
    }

    int ldb_batch_delete(struct _leveldb_stuff *ldbs, const char *key, size_t klen)
    {
        return binlog_batch_delete(ldbs, key, klen);
    }

    int ldb_batch_commit(struct _leveldb_stuff *ldbs)
    {
        return binlog_batch_commit(ldbs);
    }

    int ldb_exists(struct _leveldb_stuff *ldbs, const char *key, size_t klen)
    {
        char *err = NULL;
        char *val = NULL;
        size_t vlen = 0;
        val = leveldb_get(ldbs->db, ldbs->roptions, key, klen, (size_t *) &vlen, &err);
        if (err) {
            log_error("%s", err);
            leveldb_free(err);
            err = NULL;
            return -1;
        }
        leveldb_free(val);

        return vlen ? 1 : 0;
    }

    int ldb_syncset(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen)
    {
        return binlog_syncset(ldbs, key, klen, value, vlen);
    }

    int ldb_syncdel(struct _leveldb_stuff *ldbs, const char *key, size_t klen)
    {
        return binlog_syncdel(ldbs, key, klen);
    }

    int ldb_compact(struct _leveldb_stuff *ldbs)
    {
        leveldb_compact_range(ldbs->db, NULL, 0, NULL, 0);
        return 0;
    }
