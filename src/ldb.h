/*
 * ldb.h
 *
 *  Created on: May 24, 2014
 *      Author: chenjianfei
 */

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/types.h>

#include "leveldb/c.h"

#define SOME_KV_NODES_COUNT 1024

struct kv_node {
    int klen;
    int vlen;
    char *key;
    char *val;
};

struct some_kv {
    struct kv_node nodes[ SOME_KV_NODES_COUNT ];
    struct some_kv *next;
    struct some_kv *prev;
};

struct kv_list {
    int count;
    int klens;
    int vlens;
    int knubs;
    int vnubs;
    struct some_kv head;
};

//#define DEFAULT_DBNAME_SIZE 200
#define DEFAULT_DBNAME_SIZE 1024

struct _leveldb_stuff {
    leveldb_t* db;
    leveldb_options_t *options;
    leveldb_readoptions_t *roptions;
    leveldb_writeoptions_t *woptions;
    leveldb_writebatch_t *wbatch;       // not safe.
    char dbname[DEFAULT_DBNAME_SIZE];
};


int get_number_len(size_t len);
char *set_bulk(char *dst, const char *put, int len);

/*
 * Initial or create a level-db instance.
 */
struct _leveldb_stuff *ldb_initialize(char *path);

/*
 * Close the level-db instance.
 */
void ldb_close(struct _leveldb_stuff *ldbs);

/*
 * Destroy the level-db.
 */
void ldb_destroy(struct _leveldb_stuff *ldbs);

/*
 * Get key's value.
 */
char *ldb_get(struct _leveldb_stuff *ldbs, const char *key, size_t klen, int *vlen);

/*
 * Not used.
 */
char *ldb_tsget(struct _leveldb_stuff *ldbs, const char *st_key, size_t st_klen, const char *ed_key, size_t ed_klen, int *size);

/*
 * Get a key range info.
 */
char *ldb_lrangeget(struct _leveldb_stuff *ldbs, const char *key, size_t suf_klen, const char *st_time, size_t st_tlen, const char *ed_time, size_t ed_tlen, int *size);

/*
 * Get keys.
 * Now just support prefix match.
 */
char *ldb_keys(struct _leveldb_stuff *ldbs, const char *ptn, size_t ptn_len, int *size);

/*
 * Get values.
 * Regular match.
 * XXX: Please use caution.
 */
char *ldb_values(struct _leveldb_stuff *ldbs, const char *ptn, size_t ptn_len, int *size);

/*
 * Get server info.
 */
char *ldb_info(struct _leveldb_stuff *ldbs, int *size);

/*
 * Set record (key value).
 */
inline int ldb_put(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen);

/*
 * Delete record.
 * not use.
 */
inline int ldb_delete(struct _leveldb_stuff *ldbs, const char *key, size_t klen);

/*
 * Batch set record.
 */
int ldb_batch_put(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen);

/*
 * Batch del record.
 */
int ldb_batch_delete(struct _leveldb_stuff *ldbs, const char *key, size_t klen);

/*
 * Commit batch set.
 * With ldb_batch_put to use.
 */
int ldb_batch_commit(struct _leveldb_stuff *ldbs);

/*
 * Returns if key exists.
 */
int ldb_exists(struct _leveldb_stuff *ldbs, const char *key, size_t klen);

/*
 * the following apis is mainly used for synchronize with master.
 * note: Do not call the apis directly in your program.
 */
int ldb_syncset(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen);
int ldb_syncdel(struct _leveldb_stuff *ldbs, const char *key, size_t klen);

/*
 * Compact the database.
 */
int ldb_compact(struct _leveldb_stuff *ldbs);

