/*
 * file: ctl.h
 * auth: cjfeii@126.com
 * date: Aug 8, 2014
 * desc: control process.
 */

#pragma once

#include "main.h"

#define MAX_CMD_LEN         512*1024*1024

#define OPT_OK              "+OK\r\n"
#define OPT_PONG            "+PONG\r\n"
#define OPT_NUMBER          ":%d\r\n"
#define OPT_ONE             ":1\r\n"
#define OPT_ZERO            ":0\r\n"
#define OPT_LDB_ERROR       "-LDB ERROR\r\n"
#define OPT_CMD_ERROR       "-CMD ERROR\r\n"
#define OPT_NULL            "$-1\r\n"
#define OPT_EMPTY_MULTI     "*0\r\n"
#define OPT_NO_MEMORY       "-NO MORE MEMORY!\r\n"
#define OPT_DATA_TOO_LARGE  "-DATA TOO LARGE!\r\n"

#define X_CLIENT_QUIT       3
#define X_DATA_NO_ALL       2
#define X_DATA_IS_ALL       1
#define X_DONE_OK           0
#define X_DATA_TOO_LARGE    -1
#define X_MALLOC_FAILED     -2
#define X_ERROR_CMD         -3
#define X_ERROR_LDB         -4


extern int ctl_ldb_init(char *db_name);

extern int ctl_ldb_close(void);

extern int ctl_cmd_parse(struct data_node *p_node);

extern int ctl_cmd_done(struct data_node *p_node);

extern int ctl_status_clean(struct data_node *p_node);
