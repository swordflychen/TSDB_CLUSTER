--author : chenzutao
--date : 2014-05-26 CST
--func : main function to check if  the data in redis is the same with TSDB

package.path = '/data/tsdb/TSDB/utils/mutil_data_integration/deploy/?.lua;/data/tsdb/TSDB/utils/mutil_data_integration/api/?.lua;;' .. package.path

local only = require 'only'
local cfg = require 'config'
local check = require 'CHECK_DATA'
local utils = require 'TSDB_UTILS'

local allow_sleep = cfg.ARGS['ALLOW_SLEEP']
local allow_setTime = cfg.ARGS['ALLOW_SETTIME']

local args = {}

function handle()
	print("---- check data ----")
	local st_date = cfg.ARGS['START_TIME']
	local ed_date = cfg.ARGS['END_TIME']
	local unix_time_st = utils.get_time_by_date(st_date)
	local unix_time_ed = utils.get_time_by_date(ed_date)
	if unix_time_ed - unix_time_st <= 0 then
		only.log('E', "== end time must big than start time ! ==")
		return
	end
	
	only.log('I', 'start date : ' .. st_date .. ' -- end date : ' .. ed_date)
	
	for i=unix_time_st, unix_time_ed, 600 do
		if allow_sleep == 1 then
			os.execute('sleep 600s')
		end
		args['ts'] = i
		local ret = check.handle(args)
		if not ret then
			only.log('E', 'failed to check the ' .. os.date('%Y-%m-%d %H:%M:%S', i) .. ' data')
			return nil
		end
	end
	only.log('I', 'all data between [' .. st_date .. ']  to [' .. ed_date .. '] is the same !')
	print('all data between [' .. st_date .. ']  to [' .. ed_date .. '] is the same !')
	print("======  finished  ======")
end

handle()
