package.path = './deploy/?.lua;;' .. package.path

local redis = require 'redis_pool_api'
local utils = require 'TSDB_UTILS'

local expire_time = 600

--[[
--==============================================================
--get keys and set expire time

local ok, ret = redis.cmd('tsdb_redis', 'keys', '*20140616*')

print(type(ok), type(ret))
print(tostring(ok), tostring(ret))

for k, v in pairs(ret) do
	local st, re = redis.cmd('tsdb_redis', 'ttl', v)
	--local st, re = redis.cmd('tsdb_redis', 'expire', v, expire_time)
	if not st then
		print("failed !")
	end
	print(re)
end

print("total :" .. #ret)
--===============================================================
]]--

--[[
--===============================================================
-- URL has error , should write file by human

local file = io.open('/data/tsdb/int_data/backFile/url__2014061608.bak', 'a+')
local ok, ret = redis.cmd('tsdb_redis', 'smembers', 'URL:982674641464726:20140616084')
if not ok then	
	print("error !")
	return nil
end
table.sort(ret)
local len = #ret
local tmp = len .. '*4@'
for _, v in pairs(ret) do
	v = string.gsub(v, '\r\n', '\\r\\n')
	tmp = tmp .. v .. '|'
end

--print(tmp)

file:write('URL:982674641464726:20140616084\n')
file:write(tmp)

local ok1, ret1 = redis.cmd('tsdb_w', 'set', 'URL:982674641464726:20140616084', tmp)
if not ok1 then
	print("set tsdb failed !")
	return nil
end

--=================================================================
]]--

--=================================================================
--get data from tsdb and write it into file
--
for i=17, 23 do
	local file = io.open('/data/tsdb/int_data/backFile/url__20140615' .. i .. '.bak', 'a+')
	local stat, user = redis.cmd('tsdb_r', 'get' , 'ACTIVEUSER:20140615')
	local i, j
	local tmp
	local tab = {}
	local ok, ret
	for k, v in pairs(user) do
		i, j = string.find(v, '@')
		tmp = string.sub(v, j+1, -1)
		tab = utils.split(tmp , '|')
		for j=1, #tab do
			ok, ret = redis.cmd('tsdb_r', 'get', 'URL:' .. tab[j] .. ':20140615')
			if not ok then
				print("failed : " .. tab[j] .. ': time : ' .. i)
			end
		end
	end

--print(tmp)

	file:write('URL:982674641464726:20140616084\n')
	file:write(tmp)

	local ok1, ret1 = redis.cmd('tsdb_w', 'set', 'URL:982674641464726:20140616084', tmp)
	if not ok1 then
		print("set tsdb failed !")
		return nil
	end
--===================================================================
