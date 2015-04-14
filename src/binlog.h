/*
 * file: binlog.h
 * auth: chenjianfei@daoke.me
 * date: Aug 8, 2014
 * desc: 
 */

#pragma once

#include <stdint.h>
#include "ldb.h"

#define BINLOG_MAX_KEY_SIZE 16
#define BINLOG_MAX_VAL_SIZE 512

/* op type. */
#define CMD_SET     1
#define CMD_DEL     2
#define CMD_PING    3

/* binlog. */
typedef struct _binlog
{
    uint64_t min_seq;
    uint64_t cur_seq;
    uint64_t last_seq;
} binlog_t;

typedef struct _bl_buffer
{
    char w_key[BINLOG_MAX_KEY_SIZE];
    char w_val[BINLOG_MAX_VAL_SIZE];
    size_t wk_len;
    size_t wv_len;

    char r_key[BINLOG_MAX_KEY_SIZE];
    char r_val[BINLOG_MAX_VAL_SIZE];
    size_t rk_len;
    size_t rv_len;
} bl_buf_t;

/*
 * initial/close binlog.
 */
int binlog_open(struct _leveldb_stuff *ldbs, uint8_t is_log);
void binlog_close(void);

/*
 * put.
 */
int binlog_put(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen);

/*
 * delete.
 * not use.
 */
int binlog_delete(struct _leveldb_stuff *ldbs, const char *key, size_t klen);

/*
 * batch set.
 */
int binlog_batch_put(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen);

/*
 * batch delete.
 */
int binlog_batch_delete(struct _leveldb_stuff *ldbs, const char *key, size_t klen);

/*
 * batch commit.
 */
int binlog_batch_commit(struct _leveldb_stuff *ldbs);

/*
 * sync apis.
 */
int binlog_syncset(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen);
int binlog_syncdel(struct _leveldb_stuff *ldbs, const char *key, size_t klen);

/*
 * operate the binlog.
 */
int get_binlog(struct _leveldb_stuff *ldbs, uint64_t seq, char *msg, size_t *length);
int del_binlog(struct _leveldb_stuff *ldbs, uint64_t seq);


