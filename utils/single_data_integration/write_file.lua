--author : chenzutao
--date : 2014-05-16
--
package.path = './deploy/?.lua;./api/?.lua;;' .. package.path
local only = require 'only'

local cfg = require 'config'
local url = require 'URL_TO_TSDB'
local write_rec = require 'WRITE_RECOVERY'
local UTILS = require 'TSDB_UTILS'

local args = {}

local allow_sleep = cfg.ARGS['ALLOW_SLEEP']
local allow_setTime = cfg.ARGS['ALLOW_SETTIME']
local time_file
local value_prefix = cfg.ARGS['VALUE_PREFIX']
local file_prefix = '/data/tsdb/int_data/'

local function make_file()
	os.execute('mkdir -p ' .. file_prefix .. 'recoveryFile')
	os.execute('mkdir -p ' .. file_prefix .. 'errorLog')
end

local function open_logfile()
	args['empty_user_file'] = io.open(file_prefix .. 'errorLog/emptyUser.log', 'a+')
	args['expire_time_file'] = io.open(file_prefix .. 'errorLog/expireTime.log', 'a+')
	args['error_file'] = io.open(file_prefix .. 'errorLog/error.log', 'a+')
	time_file = io.open(file_prefix .. 'recoveryFile/TIME_BUILD', 'w+')
end

local function open_recoveryfile(time)
	local url_file_name = string.format('%srecoveryFile/%s__%s.rev', file_prefix,string.lower(value_prefix), time)
	local user_file_name = string.format('%srecoveryFile/activeUser__%s.rev', file_prefix, time)
	args['url_file'] = io.open(url_file_name, 'a+')
	args['user_file'] = io.open(user_file_name, 'a+')
end

local function close_file()
	args['empty_user_file']:close()
	args['expire_time_file']:close()
	args['error_file']:close()
	args['url_file']:close()
	args['user_file']:close()
	time_file:close()
end

function handle_1()
	local st_time = os.time()
	make_file()
	open_logfile()
	--set to TSDB in a set time
	--time form : yyyy-mm-dd HH:MM:SS
	--st_date = '2014-05-13 10:33:20'
	--ed_date = '2014-05-13 19:12:07'
	local st_date = cfg.ARGS['START_TIME']
	local ed_date = cfg.ARGS['END_TIME']
	local unix_time_st = UTILS.get_time_by_date(st_date)
	local unix_time_ed = UTILS.get_time_by_date(ed_date)
	local cnt = 0
	only.log('I', "start date : " .. st_date .. ' -- end date : ' .. ed_date)	
	for i=unix_time_st, unix_time_ed + 600, 600 do
--		print("=========>> " .. os.date('%Y-%m-%d %H:%M:%S', i) .. "<<===========")
		if cnt % 6 == 0 then
			local t = os.date('%Y%m%d%H', i)
			open_recoveryfile(t)
			time_file:write(t .. '\n')
		end
		if allow_sleep == 1 then
			os.execute('sleep 600s')
		end
		args['ts'] = i
		local ret = write_rec.handle(args)
		if not ret then
			only.log('E', "failed to set the " .. os.date('%Y-%m-%d %H:%M:%S', i) .. ' to TSDB')
			close_file()
			return nil
		end
		cnt = cnt + 1
	end	
	close_file()
	local ed_time = os.time()
	only.log('I', 'total cost time :' .. (ed_time - st_time) .. ' s')
end

function handle_2()
	local st_time = os.time()
	make_file()
	open_logfile()
	local hour = 24
	local time_end = hour * 3600
	local cnt = 0
	local tmp = cfg.ARGS['DEFAULT_START_TIME']
	local time = UTILS.get_time_by_date(tmp)
	for i=time, time + time_end, 600 do
		if cnt % 6 == 0 then
			local t = os.date('%Y%m%d%H', i)
			open_recoveryfile(t)
			time_file:write(t .. '\n')
			print('the ' .. cnt/6 .. ' hour')
		end

		if allow_sleep == 1 then
			os.execute('sleep 600s')
		end

		-->> SET URL to TSDB
		args['ts'] = i
		local ret = write_rec.handle(args)
		if not ret then
			only.log('E', "failed to do the " ..  os.date('%Y-%m-%d %H:%M', i) .. ' minutes round !')
			close_file()
			return nil
		end
		cnt = cnt + 1
	end
	close_file()
	local ed_time = os.time()
	only.log('I', 'total cost time :' .. (ed_time - st_time) .. ' s')

end

if type(allow_setTime) == 'number' and allow_setTime == 1 then
--	print("正在执行自定义时间")
	handle_1()
else
	handle_2()
end
