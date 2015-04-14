-- file: test.lua
-- auth: chenjianfei@daoke.me
-- date: May 27 2014

local luazk = require("libluazk")

zkhost = "192.168.11.95:2181,192.168.11.95:2182,192.168.11.95:2183,192.168.11.95:2184,192.168.11.95:2185"
service = "URL"
key = 0

ok, ret = luazk.get_write_hosts(zkhost, service, key)

if ok then
    print("table len:".. #ret)
    for i=1, #ret do
        print(ret[i])
    end
else
    print("fuck the world.")
end


