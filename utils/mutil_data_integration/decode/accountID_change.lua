package.path = '../deploy/?.lua;../api/?.lua;;' .. package.path

local redis= require 'redis_pool_api'

local function handle()
	local ok, ret = redis.cmd('tsdb_r', 'keys', 'GSENSOR:792238124619386:')
	if not ok then
		print('failed to get GPS from TSDB')
		return nil
	end

	for i=1, #ret do
		local st, gps = redis.cmd('tsdb_r', 'get', ret[i])
		if not st or #gps==0 then
			print(string.format('failed to get gps [%s]', i))
			return nil
		end
		
		local new_str = string.gsub(gps, 'GlleRkVNyH', 'LlceXll3Ue')
		local status , result = redis.cmd('tsdb_w', 'set', ret[i], new_str)
		if not status or not result then
			print(string.format('failed to set GPS [%s]', i))
			return nil
		end
		print('count :' .. i)
	end
end
handle()
