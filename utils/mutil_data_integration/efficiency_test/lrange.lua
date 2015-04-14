
package.path = '../deploy/?.lua;../api/?.lua;;' .. package.path

local redis = require 'redis_pool_api'
local socket = require 'socket'
local link = require 'link'
local utils = require 'TSDB_UTILS'

local port_r = 'tsdb_r'

local function  handle()
	local st_u = utils.get_time_by_date('2014-06-14 00:00:00')
	local ed_u = utils.get_time_by_date('2014-06-15 23:59:59')
	local st = utils.switch_time(st_u)
	local ed = utils.switch_time(ed_u)

	print('st: ' .. st)
	print('ed: ' .. ed)
	local ok, imei = redis.cmd(port_r, 'lrange', 'ACTIVEUSER:' , st, ed)
	if not ok then
		print("failed to get activeUsers from redis")
		return nil
	end
	if not imei or #imei == 0 then
		print("have not gotten imei")
		return nil
	end
--[[
	local tmp = 0
	for i=2, #imei, 2 do
		tmp = tmp + 1 
		print(imei[i])
	end
	print(#imei)
	print(tmp)
	do return end
]]--
	local tab = {}
	local i, j
	local tmp
	local imei_tab
	local imei_cnt = 0
	for i=2, #imei, 2 do
		i, j = string.find(imei[i], '@')
		tmp = string.sub(imei[i], j+1, -1)
		imei_tab = utils.split(tmp, '|')
		table.insert(tab, imei_tab)
		imei_cnt = imei_cnt + #imei_tab
	end

	local cnt = 0
	local ok, ret
	for i=1, #tab do
	for k, v in pairs(tab[i]) do
		ok, ret = redis.cmd(port_r, 'lrange', 'GPS:' .. v .. ':', st, ed)
		if not ok then
			print("failed to get " .. v .. " from redis")
		end
		if ret and #ret > 0 then
			cnt = cnt + #ret
		end
	end
	end
	print("total data : " .. cnt)
	print("total imei cnt : " .. imei_cnt)
end
local st = socket.gettime()
handle()
local ed = socket.gettime()

print("total cost time : " .. (ed - st))
