/*
 * backup.c
 *
 *  Created on: Nov 12, 2014
 *      Author: chenjf
 *  Changed:
 *      1. if backup file is larger than a specified size,
 *      then create a new backup file and close old backup file.
 *      date:Dec 16,2014 LongQiwu
 *
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <dirent.h>

#include "backup.h"
#include "logger.h"
#include "common.h"

/* file size max: 10*1024*1024*1024. */
const uint64_t FILE_CONTENT_MAX_SIZE = (uint64_t)10 * 1024 * 1024 * 1024;

static int32_t _open_new_backup_file(backup_t *backup);

backup_t *backup_open(const char *path) {
    assert(path != NULL);

    /* allocate backup. */
    backup_t *backup = (backup_t *) calloc(1, sizeof(backup_t));

    DIR *dir = NULL;
    if ((dir = opendir(path)) == NULL ) {
        log_warn("Cannot open DB directory:[%s]", path);
        return NULL ;
    }

    /* set path. */
    strcpy(backup->path, path);

    /* get data file name. */
    int32_t i = 1, j = 1;
    struct dirent* ent = NULL;
    while ((ent = readdir(dir)) != NULL ) {
        if (ent->d_type != DT_REG) {
            continue;
        }

        if (strncmp(ent->d_name, ".", 1) == 0) {
            continue;
        }

        /* kix file find. */
        if (strncmp(ent->d_name, KIX_FILE_NAME_PATTERN, FILE_NAME_PATTERN_LEN)
                == 0) {
            if (i == 1) {
                strcpy(backup->kix_file.fname, ent->d_name);
            } else {
                strcpy(backup->kix_file.fname,
                        strcmp(ent->d_name, backup->kix_file.fname) < 0 ?
                                ent->d_name : backup->kix_file.fname);
            }
            ++i;
        }

        /* value file find. */
        if (strncmp(ent->d_name, VAL_FILE_NAME_PATTERN, FILE_NAME_PATTERN_LEN)
                == 0) {
            if (j == 1) {
                strcpy(backup->val_file.fname, ent->d_name);
            } else {
                strcpy(backup->val_file.fname,
                        strcmp(ent->d_name, backup->val_file.fname) < 0 ?
                                ent->d_name : backup->val_file.fname);
            }
            ++j;
        }
    }
    closedir(dir);

    /* check and log it. */
    assert(i == j);
    if (i == 1) {
        sprintf(backup->kix_file.fname, "%s%010d", KIX_FILE_NAME_PATTERN, 1);
        sprintf(backup->val_file.fname, "%s%010d", VAL_FILE_NAME_PATTERN, 1);
        backup->cur_idx = 1;
    } else {
        backup->cur_idx = atoi(backup->kix_file.fname + FILE_NAME_PATTERN_LEN);

        assert(backup->cur_idx > 0);
        assert(backup->cur_idx == atoi(backup->val_file.fname + FILE_NAME_PATTERN_LEN));
    }

    /* open key&index file. */
    char tmp_path[PATH_MAX];
    sprintf(tmp_path, "%s/%s", path, backup->kix_file.fname);
    backup->kix_file.fd = open(tmp_path, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
    if (backup->kix_file.fd == -1) {
        log_error("open backup key file [%s] error, errno [%d]", tmp_path, errno);
        return NULL ;
    }
    backup->kix_file.pos = get_file_size(tmp_path);

    /* open value file. */
    sprintf(tmp_path, "%s/%s", path, backup->val_file.fname);
    backup->val_file.fd = open(tmp_path, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
    if (backup->val_file.fd == -1) {
        log_error("open backup value file [%s] error, errno [%d]", tmp_path, errno);
        return NULL ;
    }
    backup->val_file.pos = get_file_size(tmp_path);

    /* open content file. */
    return backup;
}

int32_t backup_close(backup_t *backup) {
    if (backup != NULL ) {
        close(backup->kix_file.fd);
        close(backup->val_file.fd);

        free(backup);
    }

    return IX_OK;
}

int32_t backup_set(backup_t *backup, const char *key, int32_t klen, const char *val, int32_t vlen) {
    if (vlen == 0) {
        return IX_OK;
    }

    if (backup->val_file.pos + vlen > FILE_CONTENT_MAX_SIZE) {
        if (_open_new_backup_file(backup) != IX_OK) {
            return IX_ERROR;
        }
    }

    char line[KEY_BUF_SIZE + KIX_ITEM_LEN + 1] = { 0 };

    /* set value file. */
    if (write_file(backup->val_file.fd, val, vlen) != IX_OK) {
        log_error("write value file [%s] error, errno [%d]", backup->val_file.fname, errno);
        return IX_ERROR;
    }

    /* set key&index file. */
    /* FIXME: no binary safe. */
    sprintf(line, "%020ld|%010d|%s\n", backup->val_file.pos, vlen, key);
    if (write_file(backup->kix_file.fd, line, klen + KIX_ITEM_LEN) != IX_OK) {
        log_error("write key&index file [%s] error, errno [%d]", backup->kix_file.fname, errno);
        return IX_ERROR;
    }

    /* set offset. */
    backup->val_file.pos += vlen;
    backup->kix_file.pos += klen;

    /* set index. */
    return IX_OK;
}

int32_t backup_get(backup_t *backup, int32_t cursor, char *key, int32_t *klen,
        char *val, int32_t *vlen) {
    /* get index. */

    /* get content. */
    return 0;
}

int32_t backup_pop(backup_t *backup, char *key, int32_t *klen, char *val, int32_t *vlen) {
    return 0;
}

static int32_t _open_new_backup_file(backup_t *backup) {
    /*close old backup file*/
    close(backup->kix_file.fd);
    close(backup->val_file.fd);

    ++ backup->cur_idx;
    sprintf(backup->kix_file.fname, "%s%010d", KIX_FILE_NAME_PATTERN, backup->cur_idx);
    sprintf(backup->val_file.fname, "%s%010d", VAL_FILE_NAME_PATTERN, backup->cur_idx);

    /*create new file*/
    char tmp_path[PATH_MAX];
    sprintf(tmp_path, "%s/%s", backup->path, backup->kix_file.fname);
    backup->kix_file.fd = open(tmp_path, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
    if (backup->kix_file.fd == -1) {
        log_error("open backup key file [%s] error, errno [%d]", tmp_path, errno);
        return IX_ERROR;
    }
    backup->kix_file.pos = 0;

    /* open value file. */
    sprintf(tmp_path, "%s/%s", backup->path, backup->val_file.fname);
    backup->val_file.fd = open(tmp_path, O_CREAT | O_RDWR | O_APPEND, S_IRUSR | S_IWUSR);
    if (backup->val_file.fd == -1) {
        log_error("open backup value file [%s] error, errno [%d]", tmp_path, errno);
        return IX_ERROR;
    }
    backup->val_file.pos = 0;
    return IX_OK;
}

