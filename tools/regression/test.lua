-- 
-- file: test.lua
-- auth: chenjianfei@daoke.me
-- date: 2014-07-1
-- desc: regression test of tsdb
--
package.path = "../api/?.lua;./api/?.lua;" .. package.path
local redis_pool = require("redis")

-->> create a connection to tsdb.
local function connect(ip, port)
    print("============================================")
    print("connect tsdb:")
    print("ip:", ip)
    print("port:", port)

    local conn = redis_pool.connect(ip, port)

    if not conn then
        print("connect tsdb error.")
        return false
    end
    
    print("connect tsdb OK.")
    return conn
end

-->> set and get 
local function set_test(conn, key, value)
    print("============================================")
    print("set", key, value)
    local ok = conn:set(key, value)
    if not ok then
        print("set error.")
        return false
    end
    
    print("get", key)
    local value = conn:get(key)
    if not value then
        print("get error.")
        return false
    end

    print("value:", value)
    return true
end

-->> mset and get
local function mset_test(conn, key1, value1, key2, value2)
    print("============================================")
    print("mset", key1, value1, key2, value2)
    local ok = conn:mset(key1, value1, key2, value2)
    if not ok then
        print("mset error.")
        return false
    end

    print("get", key1)
    local ret_value = conn:get(key1)
    if not ret_value then
        print("get error.")
        return false
    end
    print("value:", ret_value)

    print("get", key2)
    ret_value = conn:get(key2)
    if not ret_value then
        print("get error.")
        return false
    end
    print("value:", ret_value)

    return true
end

-->> exists
local function exists_test(conn, key_exists, key_noexists)
    print("============================================")
    print("exists", key_exists)
    local ok = conn:exists(key_exists)
    if not ok then
        print("exists error.")
        return false
    end
    print(key_exists, "exists")

    print("exists", key_noexists)
    ok = conn:exists(key_noexists)
    if ok then
        print("exists error.") 
        return false
    end
    print(key_noexists, "no exists")

    return true
end

-->> lrange
local function lrange_test(conn, prefix, time1, time2)
    print("============================================")
    print("lrange ", prefix, time1, time2)
    local tab = conn:lrange(prefix, time1, time2)
    if not tab then
        print("lrange error.")
        return false
    end
    
    for i=1,#tab do
        if i%2 == 1 then
            print("key:", tab[i])
        else
            print("value:", tab[i])
        end
    end

    return true
end

-->> del
local function del_test(conn, tab)
    print("============================================")
    print("del", unpack(tab))
    local ok = conn:del(unpack(tab))
    if not ok then
        print("del error.")
        return false
    end

    for i=1,#tab do
        ok = conn:get(tab[i])
        if ok then
            print("delete key: " .. tab[i] .. " error.")
            return false
        end
    end
    
    print("delete  OK")
    return true
end

-->> close
local function close(conn)
    print("============================================")
    print("test OK.")
    return true
end

local function main()

    print("============================================")
    print("regression test for tsdb")
    if #arg ~= 2 then
        print("help:")
        print("    luajit test.lua ${ip} ${port}")
        print("============================================")
        return false
    end

    local ip = arg[1]
    local port = tonumber(arg[2])

    local key_tab = {"key1", "key2", "key3"}
    local val_tab = {"welcome", "to", "tsdb"}
   
    -->> create a connection.
    local conn = connect(ip, port) 
    assert(conn)

    -->> set and get
    local ok = set_test(conn, key_tab[1], val_tab[1])
    assert(ok)

    -->> mset and get
    ok = mset_test(conn, key_tab[2], val_tab[2], key_tab[3], val_tab[3])
    assert(ok)

    -->> exist
    ok = exists_test(conn, key_tab[1], "keyx")
    assert(ok)

    -->> lrange
    ok = lrange_test(conn, "key", "1", "3")
    assert(ok)

    -->> del
    ok = del_test(conn, key_tab)
    assert(ok)

    -->> close()
    ok = close()
    assert(ok)
end

main()

