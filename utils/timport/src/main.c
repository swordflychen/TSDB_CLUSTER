/*
 * main.c
 *
 *  Created on: Nov 12, 2014
 *      Author: chenjf
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "logger.h"
#include "backup.h"
#include "timport.h"
#include "common.h"
#include "../timport_conf.h"

int64_t get_stime(int32_t fd);
int32_t set_stime(int32_t fd, int64_t t);

int main(int argc, char **argv)
{
    /* open log. */
    int32_t ret = open_log(LOG_FILE, LOG_LEVEL, 0, 256*1024*1024);
    if (ret != 0) {
        perror("Open log error.\n");
        exit(EXIT_FAILURE);
    }

    /* open timport. */
    ret = timport_open(STATS_PATH, BACKUP_PATH, REDIS, REDIS_SIZE, TSDB, TSDB_SIZE, &STATS);
    if (ret != IX_OK) {
        log_fatal("Open timport error.");
        close_log();
        exit(EXIT_FAILURE);
    }

    /* open start time file. */
    int32_t stfd = open(START_TIME_FILE, O_RDWR, S_IRUSR | S_IWUSR);
    if (stfd == -1) {
        printf("open file:[%s] error, errno:[%d]\n", START_TIME_FILE, errno);
        exit(EXIT_FAILURE);
    }

    /* get start time. */
    int64_t time_deal = get_stime(stfd);

    /* do loop. */
    time_t now;
    while (1) {
        time(&now);
        if (now - time_deal <= 7200) { // 2 hour.
            sleep(60);
            continue;
        }

        if (timport_urllog(time_deal, TIME_LIMIT) != IX_OK) {
            log_error("timport_urllog error.");
            break;
        }
        if (timport_exurllog(time_deal, TIME_LIMIT) != IX_OK) {
            log_error("timport_urllog error.");
            break;
        }

        time_deal += 600;

        /* set start time file. */
        set_stime(stfd, time_deal);
    }

    /* close timport. */
    timport_close();

    /* close log. */
    close_log();

    return 0;
}

int64_t get_stime(int32_t fd)
{
    char buf[16] = {0};
    ssize_t size = pread(fd, buf, 16, 0);
    if (size == -1) {
            printf("read start time error, errno:[%d].\n", errno);
            exit(EXIT_FAILURE);
    }

    /* check start time. */
    uint64_t st = atoll(buf);
    if ( st < 1000000000 || st > 2000000000) {
            printf("start time:[%ld] is error.\n", st);
            exit(EXIT_FAILURE);
    }

    return st;
}

int32_t set_stime(int32_t fd, int64_t t)
{
    char buf[16] = {0};
    sprintf(buf, "%ld", t);
    ssize_t size = pwrite(fd, buf, strlen(buf), 0);
    if (size != 10) {
        printf("write start time is error, errno:[%d].\n", errno);
        exit(EXIT_FAILURE);
    }

    return 0;
}

