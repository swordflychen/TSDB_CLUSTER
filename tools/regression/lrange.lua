-- 
-- file: test.lua
-- auth: chenjianfei@daoke.me
-- date: 2014-07-1
-- desc: regression test of tsdb
--

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

-->> lrange
local function lrange_test(conn, prefix, time1, time2)
    print("============================================")
    print("lrange ", prefix, time1, time2)
    local tab = conn:lrange(prefix, time1, time2)
    
    print(type(tab))
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


-->> close
local function close(conn)
    print("============================================")
    print("test OK.")
    return true
end

local function main()

    print("============================================")
    print("regression test for tsdb")
    local ip = "192.168.1.12"
    local port = 7502
    -- local port = 6379
    local key_tab = {"key1", "key2", "key3"}
    local val_tab = {"welcome", "to", "tsdb"}
   
    -->> create a connection.
    local conn = connect(ip, port) 
    assert(conn)

    -->> lrange
    ok = lrange_test(conn, "key", "1", "3")
    assert(ok)

    -->> close()
    ok = close()
    assert(ok)
end

main()

