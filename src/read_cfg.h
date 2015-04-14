#pragma once

/* node type. */
enum node_type {
    SINGLE = 1,
    CLUSTER = 2,
};

/* node type. */
enum role_type {
    MASTER = 1,
    SLAVE = 2,
};

struct cfg_info {
    int node_type;
    char *mode;				    // mode, RO or RW.

    int ds_id;
    long key_start;
    long key_end;
    long start_time;		    // time range: start time.
    long end_time;			    // time range: end time.
    char *zk_server;		    // zookeeper server.

    char *ip;				    // IP address.
    short w_port;			    // write port.
    short r_port;			    // read port.
    size_t max_connect;		    // max connect count.
    short num_threads;		    // max thread count.

    char *work_path;		    // work space directory.
    char *log_path;			    // log directory.
    char *log_file;			    // log file name format.
    short log_verbosity;	    // log level.

    short ldb_readonly_switch;	// read only switch <=> mode
    size_t ldb_write_buffer_size;
    size_t ldb_block_size;
    size_t ldb_cache_lru_size;
    short ldb_bloom_key_size;
    int ldb_compaction_speed;

    char has_slave;              // startup slave thread or not.
    int role;
    char *slave_ip;             // slave IP address.
    short slave_wport;          // slave write port, used for sync.
};

extern void read_cfg(struct cfg_info *p_cfg, char *cfg_file);
