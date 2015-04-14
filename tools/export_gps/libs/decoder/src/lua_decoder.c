#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <string.h>
#include <stdlib.h>

#include "decode_impl.h"

char *getfield(lua_State *L, int key) 
{ 
    char *result = 0;  
    lua_pushnumber(L, key); 
    lua_gettable(L, -2); 

    if (!lua_isstring(L, -1)) {
        printf("error");
    }   
    result = (char *)lua_tostring(L, -1); 

    lua_pop(L, 1); 
    return result; 
} 

/*
 * input: 1: table size(integer);
 *        2: data table(table);
 *
 * output: 1: if succeed return table[x][y], else return nil;
 *
 */
static int decode(lua_State *L)
{
    /* parse parameters. */
    int tab_cnt = lua_tointeger(L, 1);
    const char **data = (const char **) malloc(sizeof(const char **) * tab_cnt);
    int i, j, k;
    for (i=0; i<tab_cnt; ++i) {
        data[i] = getfield(L, i+1);
    }

    /* decode. */
    int x, y, index = 1;

    Property **pro = (Property **) malloc (sizeof(Property **) * tab_cnt);
    lua_newtable(L);
    for (i=0; i<tab_cnt; ++i) {
        if (decode_impl(data[i], &(pro[i]), &x, &y)) {
            goto decode_fail;
        }
        for (j=0; j<x; ++j) {
            lua_pushnumber(L, index++);
            lua_newtable(L);
            for (k=0; k<y; ++k) {
                lua_pushnumber(L, k+1);
                lua_pushlstring(L, pro[i][j*y+k].data, pro[i][j*y+k].len);
                lua_settable(L, -3);
            }
            lua_settable(L, -3);
        }
    }
    goto decode_ok;

decode_fail:
    lua_settop(L,0);
    lua_pushnil(L);

decode_ok:
    for (j=0; j<i; ++j) {
        free(pro[j]);
    }
    free(pro);
    free(data);
    return 1;
}

static const luaL_Reg lib[] = {
    {"decode", decode},
    {NULL, NULL}
};

int luaopen_libluadecoder(lua_State *L) {
    luaL_register(L, "libluadecoder", lib);
    return 0;
}

