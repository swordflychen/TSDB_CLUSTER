/*
 * file: main.c
 * auth: chenjianfei@daoke.me
 * date:
 * desc: for test.
 */

#include <stdio.h>
#include "zk.h"


int main(int argc, char *argv[])
{
    const char *zkhost = "192.168.1.14:2181,192.168.1.14:2182,192.168.1.14:2183,192.168.1.14:2184,192.168.1.14:2185";
    zk_cxt_t *zc = open_zkhandler(zkhost);
    if (zc == NULL) {
        printf("open_zkhandler error.\n");
        return -1;
    }
    
    int ret = register_zkcache(zc);
    if (ret == -1) {
        close_zkhandler(zc);
        printf("register_zkcache error. \n");
        return -1;
    }

    const data_set_t *ds = NULL;
    ret = get_write_dataset(zc, 100, &ds);
    if (ret == -1) {
        printf("get_write_dataset error. \n");
    }

    ds = NULL;
    ret = get_read_dataset(zc, 100, 20130102000000, &ds);
    if (ret == -1) {
        printf("get_write_dataset error. \n");
    }

    unregister_zkcache(zc);
    close_zkhandler(zc);

    return 0;
}
