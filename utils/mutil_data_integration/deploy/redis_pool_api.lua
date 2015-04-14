local socket = require('socket')
local redis = require('redis')
local only = require('only')
local link = require('link')
local APP_LINK_REDIS_LIST = link["OWN_POOL"]["redis"]


module('redis_pool_api', package.seeall)

local MAX_RECNT = 10
local OWN_REDIS_POOLS = {}

local function new_connect(redname)
	local nb = 0
	repeat
		nb = nb + 1
		ok,OWN_REDIS_POOLS[redname] = pcall(redis.connect, APP_LINK_REDIS_LIST[redname]["host"], APP_LINK_REDIS_LIST[redname]["port"])
		if not ok then
			only.log("E", OWN_REDIS_POOLS[redname])
			only.log("I", string.format('REDIS_NAME: %s | INDEX: %s |---> Tcp:connect: FAILED!', redname, nb))
			OWN_REDIS_POOLS[redname] = nil
		end
		if nb >= MAX_RECNT then
			return false
		end
	until OWN_REDIS_POOLS[redname]
	only.log("I", string.format('REDIS_NAME: %s | INDEX: %s |---> Tcp:connect: SUCCESS!', redname, nb))
	return true
end


local function fetch_pool(redname)
	if not APP_LINK_REDIS_LIST[redname] then
		only.log("E", "NO redis named <--> " .. redname)
		return false
	end
	if not OWN_REDIS_POOLS[redname] then
		if not new_connect( redname ) then
			return false
		end
	end
	return true
end

local function flush_pool(redname)
	if OWN_REDIS_POOLS[redname] then
		OWN_REDIS_POOLS[redname].network.socket:close()
		OWN_REDIS_POOLS[redname] = nil
	end
	return new_connect(redname)
end


local function redis_cmd(redname, cmds, ...)
	----------------------------
	-- start
	----------------------------
	cmds = string.lower(cmds)
	local begin = os.time()
	----------------------------
	-- API
	----------------------------
	local stat,ret
	local cnt = true
	local index = 0
	repeat
--[[
::RETRY::
]]--
		if cnt then
			stat,ret = pcall(OWN_REDIS_POOLS[redname][ cmds ], OWN_REDIS_POOLS[redname], ...)
		end
		if not stat then
			local l = string.format("%s |--->FAILED! %s . . .", cmds, ret)
			only.log("E", l)
			cnt = flush_pool(redname)
		end
		index = index + 1
		if index >= MAX_RECNT then
			local l = string.format("do %s rounds |--->FAILED! this request failed!", MAX_RECNT)
			only.log("E", l)
			return false
		end
--[[
		if type(ret) == "boolean" and ret ~= true then	-- Temporarily failed
			goto RETRY
		end
]]--
	until stat
	----------------------------
	-- end
	----------------------------
	-- only.log("D", "use time :" .. (os.time() - begin))
	return ret
end

function init( )
	for name in pairs( APP_LINK_REDIS_LIST ) do
		local ok = new_connect( name )
		print("|-------" .. name .. " redis pool init------->" .. (ok and " OK!" or " FAIL!"))
	end
end

-->|	local args = {...}
-->|	local cmd, kv1, kv2 = unpack(args)
function cmd(redname, ...)
	-- only.log('D', string.format("START REDIS CMD |---> %f", socket.gettime()))
	-->> redname, cmd, keyvalue1, keyvalue2, ...
	-->> redname, {{cmd, keyvalue1, keyvalue2, ...}, {...}, ...}
	
	if not fetch_pool(redname) then
		return false,nil
	end

	local stat,ret,err
	if type(...) == 'table' then
		ret = {}
		for i=1,#... do
			if type((...)[i]) ~= 'table' then
				only.log("E", "error args to call redis_api.cmd(...)")
				break
			end

			stat,ret[i] = pcall(redis_cmd, redname, unpack((...)[i]))

			if not stat then err = ret[i] break end
		end
	else
		stat,ret = pcall(redis_cmd, redname, ...)

		if not stat then err = ret end
	end

	-- only.log('D', string.format("END REDIS CMD |---> %f", socket.gettime()))
	if not stat then
		only.log("E", "fialed in redis_cmd" .. tostring(err))
		return false,nil
	end
	return true,ret
end
