package.path = '/data/tsdb/TSDB/utils/mutil_data_integration/deploy/?.lua;;' .. package.path

local redis = require 'redis_pool_api'

local expire_time =  24*3600
local ok, ret = redis.cmd('tsdb_redis', 'keys', 'GSENS*20140619*')

print(type(ok), type(ret))
print(tostring(ok), tostring(ret))

for k, v in pairs(ret) do
	local st, re = redis.cmd('tsdb_redis', 'ttl', v)
	--local st, re = redis.cmd('tsdb_redis', 'expire', v, expire_time)
	if not st then
		print("failed !")
	end
	print(v ..' --> ' .. re)
end

print("total :" .. #ret)
