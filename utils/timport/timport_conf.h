/*
 * timport_conf.h
 *
 *  Created on: Nov 12, 2014
 *      Author: chenjf
 */

#include <stdint.h>
#include "./src/common.h"

#pragma once


/* redis config. */
const database_t    REDIS[] = {
                    {"192.168.1.12", 7776},
                    {"192.168.1.12", 7777},
                    {"192.168.1.12", 7778},
                    {"192.168.1.12", 7779}
};
#define REDIS_SIZE ((sizeof(REDIS))/sizeof(REDIS[0]))

/* tsdb config. */
const database_t    TSDB[] = {
                    { "192.168.1.14", 7501},
                    { "192.168.1.14", 7503}
};
#define TSDB_SIZE ((sizeof(TSDB))/sizeof(TSDB[0]))

/* statistics redis. */
const database_t    STATS = { "192.168.1.12", 7776};

/* backup path. */
const char          *BACKUP_PATH = "./var/backup";

/* statistics path. */
const char          *STATS_PATH = "./var/logs/stats.log";

/* log config. */
const char          *LOG_FILE = "./var/logs/access.log";
const int32_t       LOG_LEVEL = 5;

/* routine config. */
const int32_t       TIME_LIMIT = 21600; /* 6 hours. */
const char          *START_TIME_FILE = "./var/start_time.txt";

