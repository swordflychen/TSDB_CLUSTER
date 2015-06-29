/*
 * file: main.h
 * auth: cjfeii@126.com
 * date: Aug 8, 2014
 * desc: a simple server use libev
 */

#pragma once

#include <ev.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <sys/time.h>

#include "utils.h"


/*************************************************/
#define MAX_DEF_LEN	1024*1024
#define BACKLOG		1024

#define FETCH_MAX_CNT_MSG	"-you have time travel!\r\n"
/*************************************************/
/* An item in the connection queue. */
typedef struct conn_queue_item CQ_ITEM;
struct conn_queue_item {
    int sfd;
    int port;
    struct conn_queue_item *next;
    char szAddr[ INET_ADDRSTRLEN ];/* 255.255.255.255 */
};

/* A connection queue. */
typedef struct conn_queue CQ_LIST;
struct conn_queue {
    CQ_ITEM *head;
    CQ_ITEM *tail;
    pthread_mutex_t lock;
};

/*
 * libev default loop, with a accept_watcher to accept the new connect
 * and dispatch to WORK_THREAD.
 */
typedef struct {
    pthread_t thread_id;         /* unique ID of this thread */
    struct ev_loop *loop;     /* libev loop this thread uses */
    struct ev_io accept_watcher;   /* accept watcher for new connect */
} DISPATCHER_THREAD;

/*
 * Each libev instance has a async_watcher, which other threads
 * can use to signal that they've put a new connection on its queue.
 */
typedef struct {
    pthread_t thread_id;         /* unique ID of this thread */
    struct ev_loop *loop;     /* libev loop this thread uses */
    struct ev_async async_watcher;   /* async watcher for new connect */
    struct ev_prepare prepare_watcher;
    struct ev_check check_watcher;
    struct conn_queue new_conn_queue; /* queue of new connections to handle */
} WORK_THREAD;

enum {
    NO_WORKING = 0,
    IS_WORKING = 1,
};

struct data_node {
    /***cqi***/
    struct conn_queue_item item;
    /***ev***/
    struct ev_loop *loop;     /* libev loop this thread uses */

    ev_io io_watcher;
    int io_work_status;
    pthread_mutex_t io_lock;

    ev_timer timer_watcher;
    /***R***/
    int alsize;
    int gtlen;
    char *svbf;
    char recv[MAX_DEF_LEN];

    /***W***/
    int mxsize;
    int mxlen;
    int ptlen;
    char *sdbf;
    char send[MAX_DEF_LEN];
    /***P***/
    int kvs;
    int cmd;
    size_t klen;
    size_t vlen;

    int doptr;
    int key;
    int val;

    int val2;
    size_t vlen2;

    /***C***/
    int status;
};

/************public variable****************/
extern int G_PAGE_SIZE;
/**************public api*******************/
extern void clean_send_node(struct data_node *p_node);

extern int add_send_node(struct data_node *p_node, const char *output, int add);

extern int set_send_node(struct data_node *p_node, const char *output, int add);
