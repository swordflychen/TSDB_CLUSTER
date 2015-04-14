package.path = './deploy/?.lua;./api/?.lua;;' .. package.path
local only = require 'only'

local cfg = require 'config'
local url = require 'URL_TO_TSDB'
local write_rec = require 'WRITE_RECOVERY'
local UTILS = require 'TSDB_UTILS'

local args = {}

local allow_sleep = cfg.ARGS['ALLOW_SLEEP']
local allow_setTime = cfg.ARGS['ALLOW_SETTIME']
args['node_type'] = cfg.ARGS['NODE_TYPE']

local value_prefix = cfg.ARGS['VALUE_PREFIX']
local file_prefix = '/data/tsdb/int_data/'

local function make_file()
	os.execute('mkdir -p ' .. file_prefix .. 'backFile')
	os.execute('mkdir -p ' .. file_prefix .. 'errorLog')
end

local function open_logfile()
	local empty_user_file = io.open(file_prefix .. 'errorLog/emptyUser.log','a+')
	local expire_time_file = io.open(file_prefix .. 'errorLog/expireTime.log', 'a+')
	local error_file = io.open(file_prefix .. 'errorLog/error.log', 'a+')
	args['empty_user_file'] = empty_user_file
	args['expire_time_file'] = expire_time_file
	args['error_file'] = error_file
end
	
local function open_backfile(time)
	local url_file_name_bak = string.format('%sbackFile/%s__%s.bak', file_prefix, string.lower(value_prefix), time)
	local user_file_name_bak = string.format('%sbackFile/activeUser__%s.bak', file_prefix, time)
	local url_file = io.open(url_file_name_bak, 'a+')
	local user_file = io.open(user_file_name_bak, 'a+')
	args['url_file'] = url_file 
	args['user_file'] = user_file
end	

local function close_file()
	args['empty_user_file']:close()
	args['expire_time_file']:close()
	args['error_file']:close()
	args['url_file']:close()
	args['user_file']:close()
end
	

function handle_1()
	--set to TSDB in a set interval time
	--time format : yyyy-mm-dd HH:MM:SS
	--st_date = '2014-05-13 10:33:20'
	--ed_date = '2014-05-13 19:12:07'
	local st_time = os.time()
	make_file()
	open_logfile()
	local st_date = cfg.ARGS['START_TIME']
	local ed_date = cfg.ARGS['END_TIME']
	local unix_time_st = UTILS.get_time_by_date(st_date)
	local unix_time_ed = UTILS.get_time_by_date(ed_date)
	if unix_time_ed - unix_time_st <= 0 then
		only.log('E', "== end time must big than start time ! ==")
		return
	end
	local cnt = 0
	only.log('D', "======  start date : " .. st_date .. ' -- end date : ' .. ed_date .. '  ======')
	only.log('I', 'node type : ' .. args['node_type'])	
	for i=unix_time_st, unix_time_ed, 600 do
		local time = os.date('%Y%m%d%H', i)
		--print("=========>> " .. time .. "<<===========")
		if cnt % 6 == 0 then -- open a file every one hour
			open_backfile(time)
		end
		if allow_sleep == 1 then
			os.execute('sleep 600s')
		end
		args['ts'] = i
		local ret = url.handle(args)
		if not ret then
			only.log('E', "failed to set the " .. os.date('%Y-%m-%d %H:%M:%S', i) .. ' to TSDB')
			close_file()
			return nil
		end
		cnt = cnt + 1
	end	
	close_file()
	local ed_time = os.time()
	only.log('I', 'total cost time :' .. (ed_time - st_time) .. ' seconds')
end

function handle_2()
	local st_time = os.time()
	make_file()
	open_logfile()
	local hour = 24
	local time_end = hour * 3600 - 600
	local cnt = 0
	local tmp = cfg.ARGS['DEFAULT_START_TIME']
	only.log('D', "======== start at[ " .. tmp .. "] =======")
	only.log('I', 'node type : ' .. args['node_type'])	
	local time = UTILS.get_time_by_date(tmp)
	for i=time, time + time_end, 600 do
		if cnt % 6 == 0 then -- create a file every hour
			open_backfile(os.date('%Y%m%d%H', i))
		end
		if allow_sleep == 1 then
			os.execute('sleep 600s')
		end
	
		-->> SET URL to TSDB
		args['ts'] = i
		local ret = url.handle(args)
		if not ret then
			only.log('E', "failed to do the " ..  os.date('%Y-%m-%d %H:%M', i) .. ' minutes round !')
			close_file()
			return nil
		end
		cnt = cnt + 1
	end
	close_file()
	local ed_time = os.time()
	only.log('I', 'total cost time :' .. (ed_time - st_time) .. ' seconds')
end

if type(allow_setTime) == 'number' and allow_setTime == 1 then
--	print("正在执行区间时间")
	handle_1()
else
	handle_2()
end
