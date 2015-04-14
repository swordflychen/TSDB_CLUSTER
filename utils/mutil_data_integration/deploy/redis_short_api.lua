local socket = require "socket"
local redis = require('redis')
local only = require('only')
local link = require('link')
local APP_LINK_REDIS_LIST = link["OWN_DIED"]["redis"]

module('redis_short_api', package.seeall)

local OWN_REDIS_SHORT = {}

local function redis_cmd(redname, cmds, ...)
	cmds = string.lower(cmds)
	--local begin = os.time()
	local ok, ret = pcall(OWN_REDIS_SHORT[redname][ cmds ], OWN_REDIS_SHORT[redname], ...)
	if not ok then
		only.log("E", string.format("%s |--->FAILED! %s...", cmds, ret))
		return false
	end
	-- only.log("D", "use time :" .. (os.time() - begin))
	return ret
end

-->|	local args = {...}
-->|	local cmd, kv1, kv2 = unpack(args)
function cmd(redname, ...)
	-- only.log('D', string.format("START REDIS CMD |---> %f", socket.gettime()))
	-->> redname, cmd, keyvalue1, keyvalue2, ...
	-->> redname, {{cmd, keyvalue1, keyvalue2, ...}, {...}, ...}
	
	-->| STEP 1 |<--
	-->> fetch
	if not APP_LINK_REDIS_LIST[redname] then
		only.log("E", "NO redis named <--> " .. redname)
		return false,nil
	end
	local host = APP_LINK_REDIS_LIST[redname]["host"]
	local port = APP_LINK_REDIS_LIST[redname]["port"]
	only.log("D", string.format("%s redis is on %s:%d", redname, host, port))
	-->| STEP 2 |<--
	-->> connect
	ok,OWN_REDIS_SHORT[redname] = pcall(redis.connect, host, port)
	if (not ok) or (not OWN_REDIS_SHORT[redname]) then
		only.log("E", string.format("Failed connect redis on %s:%s", host, port))
		OWN_REDIS_SHORT[redname] = nil
		return false,nil
	end
	-->| STEP 3 |<--
	-->> do cmd
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
	-->| STEP 4 |<--
	-->> close
	OWN_REDIS_SHORT[redname]:quit()
	OWN_REDIS_SHORT[redname] = nil

	-- only.log('D', string.format("END REDIS CMD |---> %f", socket.gettime()))
	if not stat then
		only.log("E", "failed in redis_cmd" .. tostring(err))
		return false,nil
	end
	return true,ret
end
