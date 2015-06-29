/*
 * file: slave.c
 * auth: cjfeii@126.com
 * date: Aug 11, 2014
 * desc: slave opetator.
 */

#include <arpa/inet.h>
#include <errno.h>

#include "slave.h"
#include "logger.h"
#include "binlog.h"

#define SEND_BUF_SIZE       2048576
#define RECV_BUF_SIZE       512

static slave_t *slave = NULL;
static char send_buf[SEND_BUF_SIZE];        /* send buffer. */
static char recv_buf[RECV_BUF_SIZE];        /* recv buffer. */

extern binlog_t *binlog;

static int32_t sock_connect(const char *ip, int16_t port)
{
    int32_t sock = -1;
    int32_t opt = 1;

    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    if((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1){
        goto sock_err;
    }
    if(connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == -1){
        goto sock_err;
    }

    setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void *)&opt, sizeof(opt));

    slave->sock = sock;
    return 0;

sock_err:
    if(sock >= 0){
        close(sock);
    }
    slave->sock = -1;
    return -1;
}
static int32_t send_request(const char *msg, size_t len)
{
    int32_t ret;
    while (len > 0) {
        ret = send(slave->sock, msg, len, 0);
        if (ret < 0) {
            log_error("sync seq[%ld] error: send error, errno[%d]", binlog->cur_seq, errno);
            return -1;
        }

        len -= ret;
        msg += ret;
    }

    return 0;
}
static int recv_response()
{
    while (1) {
        int32_t ret = recv(slave->sock, recv_buf, RECV_BUF_SIZE, 0);
        if (ret > 0) {
            break;
        }

        if (ret == 0) {
            log_error("recv error: remote socket closed, seq:[%ld]", binlog->cur_seq);
            return -1;
        }

        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
        }

        log_error("error: recv error: remote socket broken, seq:[%ld]", binlog->cur_seq);
        return -1;
    }
    return 0;
}

static int check_response(const char *pattern)
{
    if (strstr(recv_buf, pattern) == NULL) {
        return -1;
    }
    return 0;
}

void *slave_thread(void *args)
{
    size_t len;
    int ret;

    binlog->cur_seq = binlog->min_seq;

    while (!slave->is_quit) {
        if (slave->sock == -1) {
            /* socket close, need connect or reconnect. */
            if (sock_connect(slave->ip, slave->port) == -1) {
                log_error("socket connect ip:[%s], port:[%d] failed.", slave->ip, slave->port);
                sleep(5);
                continue;
            }
            log_info("socket connect ip:[%s], port:[%d] success.", slave->ip, slave->port);
        }

        /* sleep. */
        sleep(5);

heart_beat:
        /* heart beat: PING<--->PONG. beat radio: 1 times/5second. */
        if (send_request("*1\r\n$4\r\nPING\r\n", strlen("*1\r\n$4\r\nPING\r\n")) != 0) {
            log_error("heart beat failure: slave maybe disconnect.");

            close(slave->sock);
            slave->sock = -1; /* invalid socket fd. */
            continue;
        }
        if (recv_response() != 0) {
            close(slave->sock);
            slave->sock = -1; /* invalid socket fd. */
            continue;
        }
        if (check_response("PONG") != 0) {
            log_error("recv error msg, then send again. req: [%ld], send_buf:[%s].", binlog->cur_seq, send_buf);
            goto heart_beat;
        }

        /* sync binlog. */
        while (binlog->cur_seq <= binlog->last_seq) {

            /* get binlog. */
            len = SEND_BUF_SIZE;
            ret = get_binlog(slave->ldbs, binlog->cur_seq, send_buf, &len);
            if (ret == -1) {
                log_error("get binlog req:[%ld] failed.", binlog->cur_seq);
                continue;
            } else if (ret == 0) {
                /* record expired, not need sync. */
                log_error("get binlog req:[%ld] failed.", binlog->cur_seq);
                goto delete_binlog;
            }

send_syncdata:
            /* send sync request. */
            ret = send_request(send_buf, len);
            if (ret == -1) {
                close(slave->sock);
                slave->sock = -1; /* invalid socket fd. */
                break;
            }

            /* receive response. */
            if (recv_response() != 0) {
                close(slave->sock);
                slave->sock = -1; /* invalid socket fd. */
                break;
            }

            /* check response. */
            if (check_response("OK") != 0) {
                log_error("recv error msg, then send again. req: [%ld], send_buf:[%s].", binlog->cur_seq, send_buf);
                goto send_syncdata; // TODO: continue?
            }

delete_binlog:
            /* sync ok, delete binlog. */
            ret = del_binlog(slave->ldbs, binlog->cur_seq);
            if (ret == -1) {
                log_error("delete binlog req:[%ld] failed, may need to manually synchronize it.", binlog->cur_seq);
            }

            /* FIXME: need initial the buffer. */

            ++binlog->cur_seq;
        }
    }

    return NULL;
}

int slave_open(struct _leveldb_stuff *ldbs, uint8_t is_slave, const char *ip, uint16_t port)
{
    /* set slave or not. */
    if (!is_slave) {
        return 0;
    }
    slave = (slave_t *) malloc (sizeof(slave_t));
    slave->is_quit = 0;

    slave->ldbs = ldbs;
    strcpy(slave->ip, ip);
    slave->port = port;
    slave->sock = -1;

    /* create slave thread. */
    int ret = pthread_create(&(slave->tid), NULL, slave_thread, NULL);
    if (ret != 0) {
        log_fatal("create slave thread error. errno: %d", errno);
        return -1;
    }

    return 0;
}

int slave_close(void)
{
    if (slave == NULL) {
        return 0;
    }

    if (!slave->is_quit) {
        slave->is_quit = 1;
        pthread_join(slave->tid, NULL);
    }
    return 0;
}
