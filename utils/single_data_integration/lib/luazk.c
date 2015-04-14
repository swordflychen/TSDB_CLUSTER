/*
 * zklua.c
 *
 *  Create on: May 27, 2014
 *     Author: chenjianfei@daoke.me
 */

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <string.h>

#include "zookeeper_log.h"
#include "zookeeper.h"

/*
 * get write hosts.
 *
 * input :  zkhost
 *          service
 *          key
 * output:  if succeed, return 1, else return nil
 *          if succeed, return write host(table), else return error message(string).    
 */
static int get_write_hosts(lua_State *L)
{
    const char *zkhost = lua_tostring(L, 1);
    const char *service = lua_tostring(L, 2);
    int key = lua_tointeger(L, 3);

    /* set zk log level. */
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);

    /* create a zk handle. */
    char errmsg[64] = {0};
    zhandle_t *zkhandle = NULL;
    int zktimeout = 30000;
    zkhandle = zookeeper_init(zkhost, NULL, zktimeout, 0, "lua: get write node.", 0);
    if(zkhandle == NULL) {
        lua_pushnil(L);
        lua_pushstring(L, "luazk: zk init error.");
        return 2;
    }

    /* get host. */
    struct String_vector sv;
    char node[512] = {0};
    sprintf(node, "/keys/%s/%d", service, key);
    int ret = zoo_get_children(zkhandle, node, 0, &sv);
    if(ZOK != ret) {
        sprintf(errmsg, "luazk: zoo_get_children error, errno: %d", ret);
        lua_pushnil(L);
        lua_pushstring(L, errmsg);
        return 2;
    }

    /* close zk handle.*/
    zookeeper_close(zkhandle);

    /* deal result. */
    int i;
    lua_pushboolean(L, 1);
    lua_newtable(L);
    for(i=0; i<sv.count; i++) {
        if(strstr(sv.data[i], "RW:")) {
            lua_pushstring(L, sv.data[i]);
            lua_rawseti(L, -2, i+1);
        }
    }

    return 2;
}


static const luaL_Reg lib[] = {
    {"get_write_hosts", get_write_hosts},
    {NULL, NULL}
};

int luaopen_libluazk(lua_State *L)
{
    luaL_register(L, "libluazk", lib);
    return 0;
}
