/*
 * a simple server use libev
 */

#define _GNU_SOURCE             /* See feature_test_macros(7) */
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>

#include "ctl.h"
#include "main.h"
#include "zk.h"
#include "read_cfg.h"
#include "logger.h"


int G_PAGE_SIZE = 0;

struct timeval g_dbg_time;
struct data_node *g_cache_pool = NULL;
static DISPATCHER_THREAD g_dispatcher_thread = {0}; /* dispatcher read request */
static DISPATCHER_THREAD g_master_thread = {0};     /* do write request */
static WORK_THREAD *g_work_threads = NULL;          /* do read request */

static int g_round_robin = 0;
static int g_init_count = 0;
static pthread_mutex_t g_init_lock;
static pthread_cond_t g_init_cond;
enum process_type{
    master,
    child,
}g_work_type;

/*global config*/
struct cfg_info g_cfg_pool = {};

int W_PORT = 0;
int R_PORT = 0;
int MAX_CONNECT = 0;
int NUM_THREADS = 0;

short LDB_READONLY_SWITCH = 0;
size_t LDB_WRITE_BUFFER_SIZE = 0;
size_t LDB_BLOCK_SIZE = 0;
size_t LDB_CACHE_LRU_SIZE = 0;
short LDB_BLOOM_KEY_SIZE = 0;
char *LDB_WORK_PATH = NULL;

time_t LDB_START_TIME;

static void first_init(int argc ,char** argv)
{
    if (argc < 2) {
        read_cfg(&g_cfg_pool, "./config.json");
    } else {
        read_cfg(&g_cfg_pool, argv[1]);
    }

    G_PAGE_SIZE = getpagesize();

    W_PORT = g_cfg_pool.w_port;
    R_PORT= g_cfg_pool.r_port;
    MAX_CONNECT = g_cfg_pool.max_connect;
    NUM_THREADS = g_cfg_pool.num_threads;

    LDB_WORK_PATH = g_cfg_pool.work_path;

    time(&LDB_START_TIME);

    LDB_READONLY_SWITCH		= g_cfg_pool.ldb_readonly_switch;
    LDB_WRITE_BUFFER_SIZE	= g_cfg_pool.ldb_write_buffer_size;
    LDB_BLOCK_SIZE			= g_cfg_pool.ldb_block_size;
    LDB_CACHE_LRU_SIZE		= g_cfg_pool.ldb_cache_lru_size;
    LDB_BLOOM_KEY_SIZE		= g_cfg_pool.ldb_bloom_key_size;


    open_new_log();

    check_pid_file();
}

static void pools_init()
{
    int i;
    g_cache_pool = malloc( sizeof(struct data_node) * MAX_CONNECT );
    memset( g_cache_pool, 0, sizeof(struct data_node) * MAX_CONNECT );
    for(i=0; i<MAX_CONNECT; i++){
        g_cache_pool[i].svbf = g_cache_pool[i].recv;
        g_cache_pool[i].alsize = MAX_DEF_LEN;
        g_cache_pool[i].sdbf = g_cache_pool[i].send;
        g_cache_pool[i].mxsize = MAX_DEF_LEN;
    }
}

static int add_recv_node(struct data_node *p_node, char *input, int add)
{
    char *old = p_node->svbf;
    if ( p_node->alsize < (p_node->gtlen + add) ){
        p_node->alsize += MAX_DEF_LEN;
        if ( ! (p_node->svbf = malloc(p_node->alsize))){
            return X_MALLOC_FAILED;
        }
        memset(p_node->svbf, 0,  p_node->alsize);
        memcpy(p_node->svbf, old, p_node->gtlen);
        if (old == p_node->recv){
            memset(p_node->recv, 0, MAX_DEF_LEN);
        }else{
            free(old);
        }
    }
    memcpy(p_node->svbf + p_node->gtlen, input, add);
    p_node->gtlen += add;
    return X_DONE_OK;
}

int add_send_node(struct data_node *p_node, const char *output, int add)
{
    char *old = p_node->sdbf;
    if ( p_node->mxsize < (p_node->mxlen + add) ){
        p_node->mxsize = GET_NEED_COUNT( (p_node->mxlen + add), MAX_DEF_LEN ) * MAX_DEF_LEN;
        if ( ! (p_node->sdbf = malloc(p_node->mxsize)) ){
            return X_MALLOC_FAILED;
        }
        memset(p_node->sdbf, 0,  p_node->mxsize);
        memcpy(p_node->sdbf, old, p_node->mxlen);
        if (old == p_node->send){
            memset(p_node->send, 0, MAX_DEF_LEN);
        }else{
            free(old);
        }
    }
    if (p_node->sdbf && add > 0){
        memcpy(p_node->sdbf + p_node->mxlen, output, add);
    }
    p_node->mxlen += add;
    return X_DONE_OK;
}

int set_send_node(struct data_node *p_node, const char *output, int add)
{
    p_node->mxsize = GET_NEED_COUNT( add, G_PAGE_SIZE ) * G_PAGE_SIZE;
    p_node->sdbf = (char *)output;
    p_node->mxlen = add;
    return X_DONE_OK;
}
static void clean_recv_node(struct data_node *p_node)
{
    p_node->alsize = MAX_DEF_LEN;
    p_node->gtlen = 0;
    if (p_node->svbf != p_node->recv){
        free( p_node->svbf);
        p_node->svbf = p_node->recv;
    }
    memset(p_node->recv, 0, MAX_DEF_LEN);
}

void clean_send_node(struct data_node *p_node)
{
    p_node->mxsize = MAX_DEF_LEN;
    p_node->mxlen = 0;
    p_node->ptlen = 0;
    if (p_node->sdbf != p_node->send){
        free( p_node->sdbf);
        p_node->sdbf = p_node->send;
    }
    memset(p_node->send, 0, MAX_DEF_LEN);
}


static void item_init(CQ_ITEM *item) {
    item->sfd = 0;
    item->port = 0;
    memset(item->szAddr, 0, INET_ADDRSTRLEN);
    item->next = NULL;
}
/*
 * Initializes a connection queue.
 */
static void cq_init(CQ_LIST *list) {
    pthread_mutex_init(&list->lock, NULL);
    list->head = NULL;
    list->tail = NULL;
}


/*
 * Looks for an item on a connection queue, but doesn't block if there isn't
 * one.
 * Returns the item, or NULL if no item is available
 */
static CQ_ITEM *cq_pop(CQ_LIST *list) {
    //pthread_mutex_lock(&list->lock);
    CQ_ITEM *item = list->head;
    if (item != NULL) {
        list->head = item->next;
        if (list->head == NULL)
            list->tail = NULL;
    }
    //pthread_mutex_unlock(&list->lock);
    return item;
}


/*
 * Adds an item to a connection queue.
 */
static void cq_push(CQ_LIST *list, CQ_ITEM *item) {
    item->next = NULL;
    //pthread_mutex_lock(&list->lock);
    if (list->tail == NULL)
        list->head = item;
    else
        list->tail->next = item;
    list->tail = item;
    //pthread_mutex_unlock(&list->lock);
}
/********************************************************/
static void accept_callback	(struct ev_loop *loop, ev_io *w, int revents);
static void recv_callback	(struct ev_loop *loop, ev_io *w, int revents);
static void send_callback	(struct ev_loop *loop, ev_io *w, int revents);
static void timeout_callback	(struct ev_loop *loop, ev_timer *w, int revents);
static void async_callback	(struct ev_loop *loop, ev_async *w, int revents);
static void check_callback	(struct ev_loop *loop, ev_check *w, int revents);


static void async_callback(struct ev_loop *loop, ev_async *w, int revents)
{
    pthread_mutex_lock(&(((WORK_THREAD *)(w->data))->new_conn_queue).lock);
    WORK_THREAD *p_work = (WORK_THREAD *)(w->data);
    CQ_ITEM *item = cq_pop( &(p_work->new_conn_queue) );

    if (item != NULL) {
        /*
           int leave = ev_loop_depth(loop);
           log_debug("fd : %d  ev_loop_depth : %d\n", item->sfd, leave);
           */
#ifdef OPEN_STATIC
        /* it,s a bug when use static watcher at thread */
        /* because a new fd will start before the old fd to stop when broken */
        pthread_mutex_lock(&(g_cache_pool[ item->sfd ].io_lock));
        if ( g_cache_pool[ item->sfd ].io_work_status == IS_WORKING ) {
            cq_push( &(p_work->new_conn_queue), item );
            pthread_mutex_unlock(&(g_cache_pool[ item->sfd ].io_lock));
            pthread_mutex_unlock(&(((WORK_THREAD *)(w->data))->new_conn_queue).lock);
            printf("+++++++++++0+++++++++++\n");
            return;
        }
        else {
            g_cache_pool[ item->sfd ].io_work_status = IS_WORKING;
        }
        pthread_mutex_unlock(&(g_cache_pool[ item->sfd ].io_lock));

        ev_io *p_watcher = &(g_cache_pool[ item->sfd ].io_watcher);
#else
        ev_io* p_watcher = malloc(sizeof(ev_io));/* have checked malloc() eque free() */
#endif

        ev_io_init( p_watcher, recv_callback, item->sfd, EV_READ);
        ev_io_start( loop, p_watcher );
        log_info("thread[%lu] accept: fd:[%d] addr:[%s] port:[%d]", p_work->thread_id, item->sfd, item->szAddr, item->port);
        item_init(item);
    }
    pthread_mutex_unlock(&(((WORK_THREAD *)(w->data))->new_conn_queue).lock);
}


static void check_callback(struct ev_loop *loop, ev_check *w, int revents)
{
    pthread_mutex_lock(&(((WORK_THREAD *)(w->data))->new_conn_queue).lock);
    WORK_THREAD *p_work = (WORK_THREAD *)(w->data);

    CQ_ITEM *item =  NULL;
    ev_io *p_watcher = NULL;
    do{
        item = cq_pop( &(p_work->new_conn_queue) );

        if (item != NULL) {
            log_debug("in check_callback!");
            /*
               int leave = ev_loop_depth(loop);
               log_debug("fd : %d  ev_loop_depth : %d\n", item->sfd, leave);
               */
#ifdef OPEN_STATIC
            /* it,s a bug when use static watcher at thread */
            /* because a new fd will start before the old fd to stop when broken */
            pthread_mutex_lock(&(g_cache_pool[ item->sfd ].io_lock));
            if ( g_cache_pool[ item->sfd ].io_work_status == IS_WORKING ) {
                cq_push( &(p_work->new_conn_queue), item );
                pthread_mutex_unlock(&(g_cache_pool[ item->sfd ].io_lock));
                pthread_mutex_unlock(&(((WORK_THREAD *)(w->data))->new_conn_queue).lock);
                printf("+++++++++++1+++++++++++\n");
                return;
            }
            else {
                g_cache_pool[ item->sfd ].io_work_status = IS_WORKING;
            }
            pthread_mutex_unlock(&(g_cache_pool[ item->sfd ].io_lock));

            p_watcher = &(g_cache_pool[ item->sfd ].io_watcher);
#else
            p_watcher = malloc(sizeof(ev_io));/* have checked malloc() eque free() */
#endif

            ev_io_init( p_watcher, recv_callback, item->sfd, EV_READ);
            ev_io_start( loop, p_watcher );

            log_debug("thread[%lu] accept: fd:[%d] addr:[%s] port:[%d]", p_work->thread_id, item->sfd, item->szAddr, item->port);
            item_init(item);
        }
    }while(item != NULL);
    pthread_mutex_unlock(&(((WORK_THREAD *)(w->data))->new_conn_queue).lock);
}


static void timeout_callback(struct ev_loop *loop, ev_timer *w, int revents)
{
    struct data_node *p_node = w->data;
    ev_io *p_io_w = &(p_node->io_watcher);

    log_debug("timeout");
    clean_recv_node( p_node );
    clean_send_node( p_node );
    close( p_io_w->fd );
    ev_io_stop( loop , p_io_w );
#if defined(OPEN_STATIC) && defined(OPEN_PTHREAD)
    pthread_mutex_lock( &p_node->io_lock );
    p_node->io_work_status = NO_WORKING;
    pthread_mutex_unlock( &p_node->io_lock );
#endif

    /* break this one connect */
    //ev_break(EV_A_ EVBREAK_ONE);

    /* clear timer from loop */
    //ev_unloop (EV_A_ EVUNLOOP_ONE); 
}

static void accept_callback(struct ev_loop *loop, ev_io *w, int revents)
{
    int newfd;
    int robin;
    struct sockaddr_in sin;
    socklen_t addrlen = sizeof(struct sockaddr);
    while ((newfd = accept(w->fd, (struct sockaddr *)&sin, &addrlen)) < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            /* these are transient, so don't log anything. */
            continue; 
        }
        else {
            log_debug("accept error, errno:[%s]", strerror(errno));
            return;
        }
    }
    if (newfd >= MAX_CONNECT){
        log_debug("connect out of range, fd:[%d]", newfd);
        send(newfd, FETCH_MAX_CNT_MSG, strlen(FETCH_MAX_CNT_MSG), 0);
        close(newfd);
        return;
    }
//    x_out_time(&g_dbg_time);
    fcntl(newfd, F_SETFL, fcntl(newfd, F_GETFL) | O_NONBLOCK);

    if ( !pthread_equal(g_master_thread.thread_id, pthread_self()) ) {
#ifdef OPEN_PTHREAD
        robin = g_round_robin%g_init_count;
        pthread_mutex_lock( &(g_work_threads[robin].new_conn_queue).lock );
        g_cache_pool[newfd].loop = g_work_threads[robin].loop;
        // set the new connect item
        CQ_ITEM *p_item = &(g_cache_pool[newfd].item);
        p_item->sfd = newfd;
        p_item->port = ntohs(sin.sin_port);
        inet_ntop(AF_INET, &sin.sin_addr, p_item->szAddr, INET_ADDRSTRLEN);

        // dispath to a work_thread.
ACCEPT_LOOP:
        if (!ev_async_pending( &(g_work_threads[robin].async_watcher) )) {
            // the event has not yet been processed (or even noted) by the event
            // loop? (i.e. Is it serviced? If yes then proceed to)
            // Sends/signals/activates the given ev_async watcher, that is, feeds
            // an EV_ASYNC event on the watcher into the event loop.
            cq_push( &(g_work_threads[robin].new_conn_queue) , p_item);
            ev_async_send(g_work_threads[robin].loop, &(g_work_threads[robin].async_watcher));
        }else{
//            printf("IN ACCEPT_LOOP!\n");
            goto ACCEPT_LOOP;
        }
        g_round_robin++;
        if( g_round_robin == MAX_CONNECT )
            g_round_robin = 0;
        pthread_mutex_unlock( &(g_work_threads[robin].new_conn_queue).lock );
#else
        g_cache_pool[newfd].loop = loop;
        ev_io *p_watcher = &(g_cache_pool[newfd].io_watcher);
        ev_io_init(p_watcher, recv_callback, newfd, EV_READ);
        ev_io_start(loop, p_watcher);
#endif
    }else{
        g_cache_pool[newfd].loop = loop;
        ev_io *p_watcher = &(g_cache_pool[newfd].io_watcher);
        ev_io_init(p_watcher, recv_callback, newfd, EV_READ);
        ev_io_start(loop, p_watcher);
    }

#ifdef OPEN_TIME_OUT
    log_debug("start timer!");
    ev_timer *p_timer = &(g_cache_pool[newfd].timer_watcher);;
    p_timer->data = &g_cache_pool[newfd];
    ev_timer_init(p_timer, timeout_callback, 5.5, 0. );
    ev_timer_start(loop, p_timer);
#endif
    //log_debug("socket fd :%d\n", p_watcher->fd);
}

static void recv_callback(struct ev_loop *loop, ev_io *w, int revents)
{
    int ret = 0;
    //ev_io *w_evt = NULL;
    char temp[MAX_DEF_LEN] = {0};

    struct data_node *p_node = &g_cache_pool[w->fd];
    ret = recv(w->fd, temp, MAX_DEF_LEN, 0);/* to use recv (MAX_DEF_LEN - 1) can't make it safe */
    log_debug("recv size: [%d]", ret);

    if(ret > 0){
        /* save recive data */
        p_node->status = add_recv_node(p_node, temp, ret);
        /* parse recive data */
        p_node->status = ctl_cmd_parse(p_node);
        switch (p_node->status){
            case X_DONE_OK:
            case X_DATA_NO_ALL:
                return;
            case X_DATA_IS_ALL:
                ctl_cmd_done(p_node);
                clean_recv_node(p_node);
                break;
            case X_CLIENT_QUIT:
                ctl_cmd_done(p_node);
                clean_recv_node(p_node);
                goto R_BROKEN;
            case X_DATA_TOO_LARGE:
            case X_MALLOC_FAILED:
            case X_ERROR_CMD:
            case X_ERROR_LDB:
            default:
                ctl_cmd_done(p_node);
                clean_recv_node(p_node);
                break;
        }
        //w_evt = malloc(sizeof(ev_io));
    }
    else if(ret ==0){/* socket has closed when read after */
        log_debug("remote socket closed! socket fd:[%d]", w->fd);
        goto R_BROKEN;
    }
    else{
        if(errno == EAGAIN ||errno == EWOULDBLOCK){
            return;
        } else {/* socket is going to close when reading */
            log_debug("ret:[%d], close socket fd:[%d]", ret, w->fd);
            goto R_BROKEN;
        }
    }
    ev_io_stop(loop,  w);
    ev_io_init(w, send_callback, w->fd,EV_WRITE);
    ev_io_start(loop, w);
    /* it will quickly run into  send_callback() when socket is not broken */
    return;
R_BROKEN:
#ifdef OPEN_TIME_OUT
    ev_timer_stop( loop, &(p_node->timer_watcher) );
#endif
    clean_recv_node(p_node);
    clean_send_node(p_node);
    ctl_status_clean(p_node);
    close(w->fd);
    ev_io_stop(loop, w);
#if defined(OPEN_STATIC) && defined(OPEN_PTHREAD)
    pthread_mutex_lock( &p_node->io_lock );
    p_node->io_work_status = NO_WORKING;
    pthread_mutex_unlock( &p_node->io_lock );
#endif

#if defined(OPEN_PTHREAD) && !defined(OPEN_STATIC)
    if ( !pthread_equal(g_master_thread.thread_id, pthread_self()) ) {
        free(w);
    }
#endif
    return;
}


static void send_callback(struct ev_loop *loop, ev_io *w, int revents)
{
//    x_out_time(&g_dbg_time);
    struct data_node *p_node = &g_cache_pool[w->fd];
    if (p_node->mxlen != p_node->ptlen){
        int ret = send(w->fd, p_node->sdbf + p_node->ptlen,
                p_node->mxlen - p_node->ptlen,
                0);
        if (ret < 0){
            log_error("fd: [%d] send error, errno: [%d]", w->fd, errno);
            goto S_BROKEN;
        }
        p_node->ptlen += ret;
        if (p_node->ptlen != p_node->mxlen){
            log_debug("send result no all");
            return;
        }
        else{
            log_debug("send result is all");
            clean_send_node(p_node);
            if (p_node->status < X_DONE_OK){
                goto S_BROKEN;
            }
        }
    }
#ifdef OPEN_TIME_OUT
    goto S_BROKEN;
#else
    ctl_status_clean(p_node);
    ev_io_stop(loop,  w);
    ev_io_init(w,recv_callback,w->fd,EV_READ);
    ev_io_start(loop, w);
    return;
#endif
S_BROKEN:
#ifdef OPEN_TIME_OUT
    ev_timer_stop( loop, &(p_node->timer_watcher) );
#endif
    clean_recv_node(p_node);
    clean_send_node(p_node);
    ctl_status_clean(p_node);
    close(w->fd);
    ev_io_stop(loop, w);
#if defined(OPEN_STATIC) && defined(OPEN_PTHREAD)
    pthread_mutex_lock( &p_node->io_lock );
    p_node->io_work_status = NO_WORKING;
    pthread_mutex_unlock( &p_node->io_lock );
#endif

#if defined(OPEN_PTHREAD) && !defined(OPEN_STATIC)
    if ( !pthread_equal(g_master_thread.thread_id, pthread_self()) ) {
        free(w);
    }
#endif
    return;
}


/*==================================================================================================*/
static int socket_init( int port )
{
    struct sockaddr_in my_addr;
    int listenfd;
    if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        log_fatal("socket init error.");
        exit(1);
    } 
    else{
        log_info("socket create success!");
    }

    /* set nonblock */
    fcntl(listenfd, F_SETFL, fcntl(listenfd, F_GETFL) | O_NONBLOCK);
    /* set reuseaddr */
    int so_reuseaddr = 1;
    setsockopt(listenfd,SOL_SOCKET,SO_REUSEADDR,&so_reuseaddr,sizeof(so_reuseaddr));
    bzero(&my_addr, sizeof(my_addr));
    my_addr.sin_family = PF_INET;
    my_addr.sin_port = htons( port );
    my_addr.sin_addr.s_addr = INADDR_ANY;//inet_addr("127.0.0.1");

    if (bind(listenfd, (struct sockaddr *) &my_addr, sizeof(struct sockaddr))== -1) {
        log_fatal("bind port[%d] error. errno:[%d]", port, errno);
        exit(1);
    } 
    else{
        log_info("ip bind success!");
    }

    if (listen(listenfd, BACKLOG) == -1) {
        log_fatal("listen");
        exit(1);
    } 
    else{
        log_info("listen success, port:[%d]", port);
    }
    return listenfd;
}

static void *start_pthread(void *arg)
{
    /*
     * Any per-thread setup can happen here; thread_init() will block until
     * all threads have finished initializing.
     */
    pthread_mutex_lock(&g_init_lock);
    g_init_count++;
    pthread_cond_signal(&g_init_cond);
    pthread_mutex_unlock(&g_init_lock);


    WORK_THREAD *this = arg;
    this->thread_id = pthread_self();
    ev_loop(this->loop, 0);
    return NULL;
}

static void setup_thread(void) 
{
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;

    pthread_mutex_init(&g_init_lock, NULL);
    pthread_cond_init(&g_init_cond, NULL);
    /* create threads pool */
    g_work_threads = (WORK_THREAD *)calloc(NUM_THREADS, sizeof(WORK_THREAD));
    if (!g_work_threads) {
        log_fatal("can't calloc work threads.");
        exit(1);
    }
    /* init threads pool */
    int i = 0;
    for (i = 0; i < NUM_THREADS; i++) {
        /* create threads loop */
        g_work_threads[i].loop = ev_loop_new (EVBACKEND_EPOLL | EVFLAG_NOENV);
        if (!g_work_threads[i].loop) {
            log_fatal("can't allocate event base.");
            exit(1);
        }

        /* init threads watcher */
        g_work_threads[i].async_watcher.data = &g_work_threads[i];
        ev_async_init( &(g_work_threads[i].async_watcher), async_callback);
        ev_async_start(g_work_threads[i].loop, &(g_work_threads[i].async_watcher) );/* Listen for notifications from other threads */

        /* init check watcher */
        g_work_threads[i].check_watcher.data = &g_work_threads[i];
        ev_check_init( &(g_work_threads[i].check_watcher), check_callback);
        ev_check_start(g_work_threads[i].loop, &(g_work_threads[i].check_watcher) );

        /* init queue list */
        cq_init( &(g_work_threads[i].new_conn_queue) );

        /* create thread */
        pthread_attr_init(&attr);
        if ((ret = pthread_create(&thread, &attr, (void *)start_pthread, &g_work_threads[i])) != 0) {
            log_fatal("can't create thread");
            //fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
            exit(1);
        }
    }

    /* Wait for all the threads to set themselves up before returning. */
    pthread_mutex_lock(&g_init_lock);
    while ( g_init_count < NUM_THREADS ) {
        pthread_cond_wait(&g_init_cond, &g_init_lock);
    }
    pthread_mutex_unlock(&g_init_lock);
}

static void *start_master(void *arg)
{
    log_info("create master sucess!");
    g_work_type = master;
    int listen = socket_init(W_PORT);
    g_master_thread.loop = ev_default_loop(0);
    g_master_thread.thread_id = pthread_self();

    ev_io_init( &(g_master_thread.accept_watcher), accept_callback, listen, EV_READ);
    ev_io_start( g_master_thread.loop, &(g_master_thread.accept_watcher) );

    ev_loop(g_master_thread.loop, 0);
    ev_loop_destroy(g_master_thread.loop);
    return NULL;
}

static void setup_master(void) 
{
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;

    /* create thread */
    pthread_attr_init(&attr);
    if ((ret = pthread_create(&thread, &attr, (void *)start_master, NULL)) != 0) {
        log_fatal("can't create thread, errno:[%d]", errno);
        exit(1);
    }
}

static void signal_callback(int sig)
{
    int quit = 0;
    switch(sig) {
        case SIGTERM:
            log_info("receive signal: SIGTERM.");
            quit = 1;
            break;
        case SIGINT:
            log_info("receive signal: SIGINT.");
            quit = 1;
            break;
        case SIGUSR1: // user defined.
            log_info("receive signal: SIGUSR1.");
            quit = 1;
            break;
        default:
            break;
    }

    if (quit) {
        log_info("exit the loops.");
        if (g_dispatcher_thread.loop) {
            ev_io_stop(g_dispatcher_thread.loop, &(g_dispatcher_thread.accept_watcher));
            ev_break(g_dispatcher_thread.loop, EVBREAK_ALL);
        }
        if (g_master_thread.loop) {
            ev_io_stop(g_master_thread.loop, &(g_master_thread.accept_watcher));
            ev_break(g_master_thread.loop, EVBREAK_ALL);
        }

        int i;
        for (i = 0; i < NUM_THREADS; i++) {
            if (g_work_threads && g_work_threads[i].loop) {
                ev_break(g_work_threads[i].loop, EVBREAK_ALL);
            }
        }
    }
}

static void register_signals()
{
    signal(SIGPIPE, SIG_IGN);

    signal(SIGINT, signal_callback);	// Ctrl-C
    signal(SIGTERM, signal_callback);	// kill
    signal(SIGUSR1, signal_callback);	// user defined signal.
}

static void run()
{
    int listen;

    /* setup master (write thread). */
    setup_master();//master is use ev_default_loop(0);

    /* setup child (read threads). */
    g_work_type = child;
    listen = socket_init(R_PORT);
#ifdef OPEN_PTHREAD
    g_dispatcher_thread.loop = ev_loop_new (EVBACKEND_EPOLL | EVFLAG_NOENV);//ev_loop_new(0);
    setup_thread();
#else
    g_dispatcher_thread.loop = ev_loop_new (EVBACKEND_EPOLL | EVFLAG_NOENV);
#endif
    g_dispatcher_thread.thread_id = pthread_self();

    ev_io_init( &(g_dispatcher_thread.accept_watcher), accept_callback, listen, EV_READ);
    ev_io_start( g_dispatcher_thread.loop, &(g_dispatcher_thread.accept_watcher) );

    /* start child. */
    ev_loop(g_dispatcher_thread.loop, 0);
    ev_loop_destroy(g_dispatcher_thread.loop);
}

int main(int argc, char** argv)
{
    /*pid_t pid;

    pid = fork();
    if (pid < 0) {
        printf("tsdb daemon: fork error.\n");
        exit(1);
    }

    if (pid != 0) {
        printf("tsdb startup as a daemon.\n");
        exit(0);
    }*/

    /* read config file and initial global variables. */
    first_init(argc, argv);

    /* register signals. */
    register_signals();

    /* write pid file. */
    write_pid_file();

    /* initial zookeeper.*/
    if(g_cfg_pool.node_type == CLUSTER) {
        if(0 != zk_init("Hello zookeeper.")) {
            exit(EXIT_FAILURE);
        }
    }

    /* initial the memory pool. */
    pools_init();

    /* initial the level db and start it. */
    if(0 != ctl_ldb_init("data")) {
        log_fatal("failed to initial the leveldb.");
        exit(EXIT_FAILURE);
    }

    /* register the data node to zookeeper. */
    if(g_cfg_pool.node_type == CLUSTER) {
        if(0 != zk_register_node()) {
            log_fatal("failed to register the node to zookeeper.");
            exit(EXIT_FAILURE);
        }
    }

    /* run loops. */
    run();

    /* unregister the data node to zookeeper. */
    if(g_cfg_pool.node_type == CLUSTER) {
        zk_unregister_node();
        zk_close();
    }

    /* close ldb. */
    ctl_ldb_close();

    /* remove pid file. */
    remove_pid_file();

    log_info("exit tsdb.");
    return 0;
}
