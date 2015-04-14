/*
 * zk.h
 *
 *  Created on: May 24, 2014
 *      Author: chenjianfei
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
