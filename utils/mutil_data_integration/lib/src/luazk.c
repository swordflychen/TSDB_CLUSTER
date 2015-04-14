/*
 * file: zklua.c
 * auth: chenjainfei@daoke.me
 * date: May 27, 2014
 * desc: not thread safe. sync with zk.h.
 */

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <assert.h>

#include "zk.h"

#define ERR_MSG_LEN 64

/*
 * open a zookeeper handle.
 * input:       nil
 * output:      1. if succeed return zkhandle(integer), else return nil.
 *              2. if succeed return nil, else return error msg.
 */
static int open_zk(lua_State *L)
{
    char errmsg[ERR_MSG_LEN] = { 0 };

    /* parse input parameters. */
    const char *zkhost = lua_tostring(L, 1);

    /* open handler. */
    zk_cxt_t *zc = open_zkhandler(zkhost);
    if (zc == NULL ) {
        sprintf(errmsg, "open zk handler failed.");
        lua_pushnil(L);
        lua_pushstring(L, errmsg);
        return 2;
    }

    /* register cache. */
    int ret = register_zkcache(zc);
    if (ret == -1) {
        close_zkhandler(zc);
        sprintf(errmsg, "register zk cache failed.");
        lua_pushnil(L);
        lua_pushstring(L, errmsg);
        return 2;
    }

    /* returns. */
    lua_pushinteger(L, (long)zc);
    lua_pushnil(L);
    return 2;
}

/*
 * close the zookeeper handle.
 * input:       1. zkhandle(integer)
 * output:      1. return true.
 */
static int close_zk(lua_State *L)
{
    /* parse input parameters. */
    zk_cxt_t *zc = (zk_cxt_t *) lua_tointeger(L, 1);

    /* unregister cache. */
    unregister_zkcache(zc);

    /* close handler. */
    close_zkhandler(zc);

    /* returns. */
    lua_pushboolean(L, 1);
    return 1;
}

/*
 * get write hosts.
 * input:   1. cache-handle(integer)
 *          2. key(integer)
 * output:  1. if succeed return host(table), else return nil
 *          2. if succeed return nil, else return error message(string).
 */
static int get_write_hosts(lua_State *L)
{
    char errmsg[ERR_MSG_LEN] = { 0 };

    /* parse input parameters. */
    zk_cxt_t *zc = (zk_cxt_t *) lua_tointeger(L, 1);
    uint64_t key = lua_tointeger(L, 2);

    /* get write dataset. */
    const data_set_t *ds = NULL;
    int ret = get_write_dataset(zc, key, &ds);
    if (ret == -1) {
        sprintf(errmsg, "get_write_dataset error.");
        lua_pushnil(L);
        lua_pushstring(L, errmsg);
        return 2;
    }

    if (ds->dn_cnt != DN_PER_DS) {
        sprintf(errmsg, "write hosts count less than or more then [%d].", DN_PER_DS);
        lua_pushnil(L);
        lua_pushstring(L, errmsg);
        return 2;
    }

    /* returns. */
    lua_newtable(L);
    int i;
    for (i=0; i<DN_PER_DS; ++i) {
        lua_pushnumber(L, i+1);
        lua_newtable(L);

        lua_pushstring(L, "ip");
        lua_pushstring(L, ds->data_node[i].ip);
        lua_settable(L, -3);

        lua_pushstring(L, "wport");
        lua_pushinteger(L, ds->data_node[i].w_port);
        lua_settable(L, -3);

        /* FIXME: need confirm. */
        lua_pushstring(L, "rport");
        lua_pushinteger(L, ds->data_node[i].r_port);
        lua_settable(L, -3);

        lua_pushstring(L, "role");
        lua_pushstring(L, ds->data_node[i].role);
        lua_settable(L, -3);

        lua_pushstring(L, "stime");
        lua_pushinteger(L, ds->s_time);
        lua_settable(L, -3);

        lua_pushstring(L, "etime");
        lua_pushinteger(L, ds->e_time);
        lua_settable(L, -3);

        lua_settable(L, -3);
    }

    return 1;
}


/*
 * get read hosts.
 * input:   1. cache-handle(integer)
 *          2. key(integer)
 *          3. time(integer)
 * output:  1. if succeed return host(table), else return nil
 *          2. if succeed return nil, else return error message(string).
 */
static int get_read_hosts(lua_State *L)
{
    char errmsg[ERR_MSG_LEN] = { 0 };

    /* parse input parameters. */
    zk_cxt_t *zc = (zk_cxt_t *) lua_tointeger(L, 1);
    uint64_t key = lua_tointeger(L, 2);
    uint64_t time = lua_tointeger(L, 3);

    /* get write dataset. */
    const data_set_t *ds = NULL;
    int ret = get_read_dataset(zc, key, time, &ds);
    if (ret == -1) {
        sprintf(errmsg, "get_read_dataset error.");
        lua_pushnil(L);
        lua_pushstring(L, errmsg);
        return 2;
    }

    if (ds->dn_cnt == 0) {
        sprintf(errmsg, "no find the hosts.");
        lua_pushnil(L);
        lua_pushstring(L, errmsg);
        return 2;
    }

    /* returns. */
    lua_newtable(L);
    int i;
    for (i=0; i<DN_PER_DS; ++i) {
        lua_pushnumber(L, i+1);
        lua_newtable(L);

        lua_pushstring(L, "ip");
        lua_pushstring(L, ds->data_node[i].ip);
        lua_settable(L, -3);

        /* FIXME: need confirm. */
        lua_pushstring(L, "wport");
        lua_pushinteger(L, ds->data_node[i].w_port);
        lua_settable(L, -3);

        lua_pushstring(L, "rport");
        lua_pushinteger(L, ds->data_node[i].r_port);
        lua_settable(L, -3);

        lua_pushstring(L, "role");
        lua_pushstring(L, ds->data_node[i].role);
        lua_settable(L, -3);

        lua_pushstring(L, "stime");
        lua_pushinteger(L, ds->s_time);
        lua_settable(L, -3);

        lua_pushstring(L, "etime");
        lua_pushinteger(L, ds->e_time);
        lua_settable(L, -3);


        lua_settable(L, -3);
    }

    return 1;
}


static const luaL_Reg lib[] = {
    { "open_zk", open_zk },
    { "close_zk", close_zk },
    { "get_write_hosts", get_write_hosts },
    { "get_read_hosts", get_read_hosts },
    { NULL,NULL }
};

int luaopen_libluazk(lua_State *L)
{
    luaL_register(L, "libluazk", lib);
    return 0;
}
