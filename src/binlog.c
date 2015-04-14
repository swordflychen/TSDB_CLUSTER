/*
 * file: binlog.c
 * auth: chenjianfei@daoke.me
 * date: Aug 8, 2014
 * desc: 
 */

#include "binlog.h"
#include <string.h>
#include <limits.h>

#include "leveldb/c.h"
#include "logger.h"



binlog_t *binlog = NULL;
bl_buf_t *bl_buf = NULL;

/* big endian function. */
static inline uint16_t big_endian_u16(uint16_t v){
    return (v>>8) | (v<<8);
}

static inline uint32_t big_endian_u32(uint32_t v){
    return (v >> 24) | ((v >> 8) & 0xff00) | ((v << 8) & 0xff0000) | (v << 24);
}

static inline uint64_t big_endian_u64(uint64_t v){
    uint32_t h = v >> 32;
    uint32_t l = v & 0xffffffffull;
    return big_endian_u32(h) | ((uint64_t)big_endian_u32(l) << 32);
}

static inline size_t encode_seq_key(uint64_t seq, char *key)
{
    size_t len = sizeof(uint64_t) + 1;
    key[0] = 0xff;

    seq = big_endian_u64(seq);
    memcpy(key + 1, &seq, len - 1);

    return len;
}

static inline uint64_t decode_seq_key(const char *key, size_t klen)
{
    if (klen != sizeof(uint64_t) + 1) {
        return -1;
    }
    return big_endian_u64(*(uint64_t *)(key + 1));
}

static inline size_t encode_seq_value(const char *val, size_t val_len, uint64_t seq, char cmd, char *seq_val)
{
    size_t seq_val_len = 1 + val_len;
    seq_val[0] = cmd;
    memcpy(seq_val + 1, val, val_len);

    return seq_val_len;
}

/* need free(*val). */
static inline size_t decode_seq_value(const char *seq_val, size_t seq_val_len, char *val, char *cmd)
{
    if (seq_val_len < 1) {
        return -1;
    }
    size_t val_len = seq_val_len - 1;
    *cmd = seq_val[0];
    memcpy(val, seq_val + 1, val_len);

    return val_len;
}

int binlog_open(struct _leveldb_stuff *ldbs, uint8_t is_log)
{
    /* set is_log. */
    if (is_log == 0) {
        return 0;
    }

    binlog = (binlog_t *) malloc (sizeof(binlog_t));
    binlog->min_seq = 0;    /* FIXME */
    binlog->last_seq = 1;

    bl_buf = (bl_buf_t *) malloc (sizeof(bl_buf_t));
    const char *tmp_key;
    size_t tmp_len;

    /* get min_seq. */
    bl_buf->wk_len = encode_seq_key(binlog->min_seq, bl_buf->w_key);
    leveldb_iterator_t* iter = leveldb_create_iterator(ldbs->db, ldbs->roptions);
    leveldb_iter_seek(iter, bl_buf->w_key, bl_buf->wk_len);
    if (leveldb_iter_valid(iter)) {
        tmp_key = leveldb_iter_key(iter, &tmp_len);
        binlog->min_seq = decode_seq_key(tmp_key, tmp_len);
        if (binlog->min_seq == -1) {
            binlog->min_seq = 1;
        }
    } else {
        binlog->min_seq = 1;
    }

    /* get last_seq. */
    leveldb_iter_seek_to_last(iter);
    if (leveldb_iter_valid(iter)) {
        tmp_key = leveldb_iter_key(iter, &tmp_len);
        binlog->last_seq = decode_seq_key(tmp_key, tmp_len);
        if (binlog->last_seq == -1) {
            binlog->last_seq = 0;
        }
    } else {
        binlog->last_seq = 0;
    }

    leveldb_iter_destroy(iter);

    return 0;
}

void binlog_close(void)
{
    if (binlog != NULL) {
        free(binlog);
        binlog = NULL;
    }
}

int binlog_put(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen)
{
    char *err = NULL;

    if (binlog != NULL) {
        /* write log. */
        uint64_t seq = binlog->last_seq + 1;
        bl_buf->wk_len = encode_seq_key(seq, bl_buf->w_key);
        bl_buf->wv_len = encode_seq_value(key, klen, seq, CMD_SET, bl_buf->w_val);

        leveldb_writebatch_put(ldbs->wbatch, bl_buf->w_key, bl_buf->wk_len, bl_buf->w_val, bl_buf->wv_len);
    }

    /* write kv-data. */
    leveldb_writebatch_put(ldbs->wbatch, key, klen, value, vlen);
    leveldb_write(ldbs->db, ldbs->woptions, ldbs->wbatch, &err);
    leveldb_writebatch_clear(ldbs->wbatch);

    if (err) {
        log_error("%s", err);
        leveldb_free(err);
        err = NULL;
        return -1;
    }

    /* notify slave thread to sync. */
    if (binlog != NULL) {
        ++ binlog->last_seq;
    }

    return 0;
}

int binlog_delete(struct _leveldb_stuff *ldbs, const char *key, size_t klen)
{
    char *err = NULL;

    if (binlog != NULL) {
        /* write log. */
        uint64_t seq = binlog->last_seq + 1;
        bl_buf->wk_len = encode_seq_key(seq, bl_buf->w_key);
        bl_buf->wv_len = encode_seq_value(key, klen, seq, CMD_DEL, bl_buf->w_val);

        leveldb_writebatch_put(ldbs->wbatch, bl_buf->w_key, bl_buf->wk_len, bl_buf->w_val, bl_buf->wv_len);
    }

    /* delete kv-data. */
    leveldb_writebatch_delete(ldbs->wbatch, key, klen);
    leveldb_write(ldbs->db, ldbs->woptions, ldbs->wbatch, &err);
    leveldb_writebatch_clear(ldbs->wbatch);

    if (err) {
        log_error("%s", err);
        leveldb_free(err);
        err = NULL;
        return -1;
    }

    /* notify slave thread to sync. */
    if (binlog != NULL) {
        ++ binlog->last_seq;
    }

    return 0;
}

int binlog_batch_put(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen)
{
    if (binlog != NULL) {
        /* write log. */
        uint64_t seq = ++ binlog->last_seq;
        bl_buf->wk_len = encode_seq_key(seq, bl_buf->w_key);
        bl_buf->wv_len = encode_seq_value(key, klen, seq, CMD_SET, bl_buf->w_val);

        leveldb_writebatch_put(ldbs->wbatch, bl_buf->w_key, bl_buf->wk_len, bl_buf->w_val, bl_buf->wv_len);
    }
    leveldb_writebatch_put(ldbs->wbatch, key, klen, value, vlen);
    return 0;
}

int binlog_batch_delete(struct _leveldb_stuff *ldbs, const char *key, size_t klen)
{
    if (binlog != NULL) {
        /* write log. */
        // TODO
        uint64_t seq = ++ binlog->last_seq;
        bl_buf->wk_len = encode_seq_key(seq, bl_buf->w_key);
        bl_buf->wv_len = encode_seq_value(key, klen, seq, CMD_DEL, bl_buf->w_val);

        leveldb_writebatch_put(ldbs->wbatch, bl_buf->w_key, bl_buf->wk_len, bl_buf->w_val, bl_buf->wv_len);
    }
    leveldb_writebatch_delete(ldbs->wbatch, key, klen);
    return 0;
}

int binlog_batch_commit(struct _leveldb_stuff *ldbs)
{
    char *err = NULL;
    leveldb_write(ldbs->db, ldbs->woptions, ldbs->wbatch, &err);
    leveldb_writebatch_clear(ldbs->wbatch);
    if (err) {
        log_error("%s", err);
        leveldb_free(err);
        err = NULL;
        return -1;
    } else {
        return 0;
    }
}

int binlog_syncset(struct _leveldb_stuff *ldbs, const char *key, size_t klen, const char *value, size_t vlen)
{
    char *err = NULL;
    leveldb_put(ldbs->db, ldbs->woptions, key, klen, value, vlen, &err);
    if (err) {
        log_error("%s", err);
        leveldb_free(err);
        err = NULL;
        return -1;
    } else {
        return 0;
    }
}
int binlog_syncdel(struct _leveldb_stuff *ldbs, const char *key, size_t klen)
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

    /* if not found, then return 0. */
    if (vlen == 0) {
        return 0;
    }

    /* if found, delete it, then return 1. */
    leveldb_delete(ldbs->db, ldbs->woptions, key, klen, &err);
    if (err) {
        log_error("%s", err);
        leveldb_free(err);
        err = NULL;
        return -1;
    }

    return 1;
}

int get_binlog(struct _leveldb_stuff *ldbs, uint64_t seq, char *msg, size_t *length)
{
    char *err = NULL;
    char *bin_value;
    size_t bvlen;
    char cmd;
    char *value;
    size_t vlen;
    char *tmp;

    /* get binlog. */
    bl_buf->rk_len = encode_seq_key(seq, bl_buf->r_key);
    bin_value = leveldb_get(ldbs->db, ldbs->roptions, bl_buf->r_key, bl_buf->rk_len, &bvlen, &err);
    if (err) {
        log_error("leveldb_get error: %s", err);
        leveldb_free(err);
        err = NULL;
        return -1;
    }

    if (bvlen == 0) {
        log_debug("not find the binlog of req: [%lu]", seq);
        *length = 0;
        return 0;
    }

    /* get binlog corresponding key. */
    bl_buf->rv_len = decode_seq_value(bin_value, bvlen, bl_buf->r_val, &cmd);
    leveldb_free(bin_value);

    if (bl_buf->rv_len == -1) {
        log_error("leveldb_get error: %s", err);
        return -1;
    }

    /* set-cmd. */
    if (cmd == CMD_SET) {
        /* get value by key. */
        value = leveldb_get(ldbs->db, ldbs->roptions, bl_buf->r_val, bl_buf->rv_len, &vlen, &err);
        if (err) {
            log_error("leveldb_get key: [%s], error: %s", bl_buf->r_val, err);
            leveldb_free(err);
            err = NULL;
            return -1;
        }

        if (vlen == 0) {
            log_info("the key [%s] may be deleted, no need sync it.", bl_buf->r_val);
            *length = 0;
            return 0;
        }

        // *3  \r\n  $7  \r\n  SYNCSET  \r\n  $3  \r\n  key  \r\n  $5  \r\n  value  \r\n
        bvlen = 2 + 3 + 7*2 + 1 + strlen("SYNCSET") + get_number_len(bl_buf->rv_len) + bl_buf->rv_len + get_number_len(vlen) + vlen;
        //       *3   $   \r\n  7   SYNCSET             3                                key              5                      value;
        if ( bvlen > *length ) {
            /* free the value. */
            leveldb_free(value);

            log_error("the buffer size [%lu] out of reange. req: [%lu]: key: [%s]", bvlen, seq, bl_buf->r_val);
            return -1;
        }
        *length = bvlen;

        bzero(msg, *length + 1);
        strcpy(msg, "*3\r\n$7\r\nSYNCSET\r\n");
        tmp = msg + strlen(msg);
        tmp = set_bulk(tmp, bl_buf->r_val, bl_buf->rv_len);
        tmp = set_bulk(tmp, value, vlen);

        /* free the value. */
        leveldb_free(value);

        /* check msg. */
        if (tmp - msg != bvlen) {
            log_fatal("the buffer size error, expect[%lu], actual[%lu]", bvlen, msg - tmp);
            return -1;
        }

        return 1;
    }

    /* del-cmd. */
    if (cmd == CMD_DEL) {
        // *2  \r\n  $7  \r\n  SYNCDEL  \r\n  $3  \r\n  key  \r\n
        bvlen = 2 + 2 + 5*2 + 1 + strlen("SYNCDEL") + get_number_len(bl_buf->rv_len) + bl_buf->rv_len;
        //       *2   $   \r\n  7   SYNCSET             3                                key
        if ( bvlen > *length ) {
            log_error("the buffer size [%lu] out of reange. req: [%lu]", bvlen, seq);
            return -1;
        }
        *length = bvlen;

        bzero(msg, *length + 1);
        strcpy(msg, "*2\r\n$7\r\nSYNCDEL\r\n");
        tmp = msg + strlen(msg);
        tmp = set_bulk(tmp, bl_buf->r_val, bl_buf->rv_len);

        /* check msg. */
        if (tmp - msg != bvlen) {
            log_fatal("the buffer size error, expect[%lu], actual[%lu]", bvlen, msg - tmp);
            return -1;
        }

        return 1;
    }

	log_error("the binlog format error of req: [%lu]", seq);
	return -1;
}

int del_binlog(struct _leveldb_stuff *ldbs, uint64_t seq)
{
    char *err = NULL;
    bl_buf->rk_len = encode_seq_key(seq, bl_buf->r_key);
    leveldb_delete(ldbs->db, ldbs->woptions, bl_buf->r_key, bl_buf->rk_len, &err);
    if (err) {
        log_error("%s", err);
        leveldb_free(err);
        err = NULL;
        return -1;
    }

    return 0;
}
