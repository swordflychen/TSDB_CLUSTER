/*
 * file: zk.h
 * auth: cjfeii@126.com
 * date: Aug 8, 2014
 * desc: zookeeper access.
 */

#pragma once

/*
 * initial a zk server.
 */
int zk_init(const char *cxt);

/*
 * close the zk handle.
 */
void zk_close();

/*
 * register this data node.
 */
int zk_register_node(void);

/*
 * unregister this data node.
 */
int zk_unregister_node(void);
