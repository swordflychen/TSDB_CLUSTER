--
-- file: test.lua
-- auth: chenjianfei@daoke.me
-- date: May 27 2014
--

local luazk = require("libluazk")

local function test_zk_cache(zkhost)
    -->> get a zkhandle.
    local zkhandle, msg = luazk.open_zk(zkhost)
    if not zkhandle then
        print(msg)
        return false
    end
    print("zkhandle: "..zkhandle)

    -->> get write hosts for key 0(the first time: no cached).
    for key=1,8192 do
        local gpshosts, msg = luazk.get_write_hosts(zkhandle, key-1)
        if not gpshosts then
            print(msg)
        else 
            -- print("table len: ".. #gpshosts)
            for i=1,#gpshosts do
                print(key-1, i, "ip: " .. gpshosts[i]["ip"],
                "wport: " .. gpshosts[i]["wport"],
                "rport: " ..  gpshosts[i]["rport"],
                "role: " .. gpshosts[i]["role"] ,
                "stime: " .. gpshosts[i]["stime"],
                "etime: " .. gpshosts[i]["etime"] )
            end
        end
    end

    -->> sleep
    io.read()

    -->> get write hosts for key 0(the second time: cached).
    gpshosts, msg = luazk.get_read_hosts(zkhandle, 0, 20140101000000)
    if not gpshosts then
        print(msg)
    else
        for i=1,#gpshosts do
            print(0, i, "ip: " .. gpshosts[i]["ip"],
            "wport: " .. gpshosts[i]["wport"],
            "rport: " ..  gpshosts[i]["rport"],
            "role: " .. gpshosts[i]["role"] ,
            "stime: " .. gpshosts[i]["stime"],
            "etime: " .. gpshosts[i]["etime"] )
        end
    end

    -->> close zk-handle.
    luazk.close_zk(zkhandle)

    return true
end

zkhost = "192.168.1.14:2181,192.168.1.14:2182,192.168.1.14:2183,192.168.1.14:2184,192.168.1.14:2185"

print("======test: get write hosts with cache=========")
test_zk_cache(zkhost)

