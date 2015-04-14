-- 
-- file: test.lua
-- auth: chenjianfei@daoke.me
-- date: 2014-07-1
-- desc: regression test of tsdb
--

package.path = "../api/?.lua;" .. package.path
package.cpath = "./libs/?.so;" .. package.cpath
local redis_pool = require("redis")
local imei_tab = require("imei").imei_tab
local decoder = require('libluadecoder')
local crc = require("crc")

-->> create a connection to tsdb.
local function connect(ip, port)
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

local function set_test(conn, imei, file)
    local key = "GPS:" .. imei .. ":201411"
    print(key)
    local ret = conn:keys(key)
    if not ret then
        print("keys error:" .. key)
        return false
    end

    local tab = {}
    local value
    for _,v in pairs(ret) do
        value = conn:get(v)
	tab[1] = value
        local tab_ret = decoder.decode(1, tab)
	for i,gps_tab in pairs(tab_ret) do
	    if #gps_tab ~= 12 then 
		print("gps format error:" .. v)
                return false
            end
	    local line = string.format("%s%16s%16s%8s%8s%8s%16s\n", gps_tab[1], tonumber(gps_tab[7])/10000000, tonumber(gps_tab[8])/10000000, gps_tab[9], gps_tab[10], gps_tab[11], crc.calc(gps_tab[12]))
            file:write(line)
	end
    end
    
    return true
end

-->> close
local function close(conn)
    print("test OK.")
    return true
end

local function main()

    local ip = "172.16.21.136"
    local port = 7502

    -->> create a connection.
    local conn = connect(ip, port) 
    assert(conn)

    -->> set and get
    for i=1,#imei_tab do
	local file_name = "./GPS_11/GPS_" .. imei_tab[i] .. "_11.txt"
	local file = io.open(file_name, "w")
	assert(file)
        ok = set_test(conn, imei_tab[i], file)
        file:close()
	assert(ok)
    end

    -->> close()
    ok = close()
    assert(ok)
end

main()

