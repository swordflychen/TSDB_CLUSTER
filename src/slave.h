/*
 * file: slave.h
 * auth: chenjianfei@daoke.me
 * date: Aug 11, 2014
 * desc: 
 */

#pragma once

#include <pthread.h>
#include <stdint.h>

typedef struct _slave
{
    uint8_t is_quit;
    char ip[16];
    uint16_t port;
    struct _leveldb_stuff *ldbs;

    pthread_t tid;
    int32_t sock;
} slave_t;


int32_t slave_open(struct _leveldb_stuff *ldbs, uint8_t is_slave, const char *ip, uint16_t port);

int32_t slave_close(void);
