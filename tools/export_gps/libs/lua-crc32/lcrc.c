#include "crc.h"
#include "lua.h"
#include "lauxlib.h"

static int calc(lua_State *L) {
	size_t l;
	const char *s = luaL_checklstring(L, 1, &l);
	lua_pushnumber(L, (lua_Number) calculateCrc((unsigned char *) s, (unsigned) l));
	return 1;
}

static const struct luaL_reg lib[] = {
	{"calc", calc},
	{NULL, NULL}
};

LUALIB_API int luaopen_crc(lua_State *L) {
	luaL_openlib(L, "crc", lib, 0);
	return 1;
}
