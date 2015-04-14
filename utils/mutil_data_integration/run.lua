package.path = '/data/tsdb/TSDB/utils/mutil_data_integration/deploy/?.lua;/data/tsdb/TSDB/utils/mutil_data_integration/api/?.lua;/data/tsdb/TSDB/utils/mutil_data_integration/?.lua;' .. package.path
local only = require 'only'

local socket = require 'socket'
local cfg = require 'config'
local url = require 'URL_TO_TSDB'
local write_rec = require 'WRITE_RECOVERY'
local UTILS = require 'TSDB_UTILS'

local args = {}

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
	
local function open_backfile(value_pre, time)
	local url_file_name_bak = string.format('%sbackFile/%s__%s.bak', file_prefix, string.lower(value_pre), time)
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
	

function handle_1(value_pre, st_date, ed_date, date_file)
	--set to TSDB in a set interval time
	--time format : yyyy-mm-dd HH:MM:SS
	--st_date = '2014-05-13 10:33:20'
	--ed_date = '2014-05-13 19:12:07'
	local st_time = socket.gettime()
	make_file()
	open_logfile()

	local unix_time_st = UTILS.get_time_by_date(st_date)
	local unix_time_ed = UTILS.get_time_by_date(ed_date)
	if unix_time_st > st_time then
		only.log('E', 'start time is bigger than current time !')
		return nil
	elseif unix_time_ed > st_time then
		unix_time_ed = st_time
	end

	if unix_time_ed - unix_time_st <= 0 then
		only.log('E', "== end time must big than start time ! ==")
		return
	end
	local cnt = 0
	only.log('D', "======  start date : " .. st_date .. ' -- end date : ' .. ed_date .. '  ======')
	only.log('I', 'node type : ' .. args['node_type'])
	local total_data = 0, data_len	
	for i=unix_time_st, unix_time_ed, 600 do
		local time = os.date('%Y%m%d%H', i)
		--print("=========>> " .. time .. "<<===========")
		if cnt % 6 == 0 then -- open a file every one hour
			open_backfile(value_pre, time)
		end
		args['ts'] = i
		args['value_prefix'] = value_pre
		local ret, data_len = url.handle(args)
		if not ret then
			only.log('E', "failed to set the " .. os.date('%Y-%m-%d %H:%M:%S', i) .. ' to TSDB')
			close_file()
			return nil
		end
		cnt = cnt + 1
                if not data_len then
                    data_len = 0
                end
		total_data = total_data + data_len
	end	
	date_file:write(string.format('%s@START@%s\n', value_pre, os.date('%Y-%m-%d %H:%M:%S',unix_time_st + cfg.ARGS['SLEEP_TIME'] * 3600)))
	date_file:write(string.format('%s@END@%s\n', value_pre, os.date('%Y-%m-%d %H:%M:%S',unix_time_ed + cfg.ARGS['SLEEP_TIME'] * 3600)))


	close_file()
	local ed_time = socket.gettime()
	only.log('I', string.format('write ->> %s <<- data from %s to %s, total cost time : %.6f seconds, total data size: %.4f MB', value_pre, st_date, ed_date, (ed_time - st_time), total_data/1024/1024))
	return total_data, (ed_time - st_time)
end

--[[
function handle_2(value_pre)
	local st_time = os.time()
	make_file()
	open_logfile()
	local hour = 24
	local time_end = hour * 3600 - 601
	local cnt = 0
	local tmp = cfg.ARGS['DEFAULT_START_TIME']
	only.log('D', "======== start at[ " .. tmp .. "] =======")
	only.log('I', 'node type : ' .. args['node_type'])	
	local time = UTILS.get_time_by_date(tmp)
	local total_data = 0, data_len
	for i=time, time + time_end, 600 do
		if cnt % 6 == 0 then -- create a file every hour
			open_backfile(value_pre, os.date('%Y%m%d%H', i))
		end
		if allow_sleep == 1 then
			os.execute('sleep 600s')
		end
	
		-->> SET URL to TSDB
		args['ts'] = i
		args['value_prefix'] = value_pre
		local ret, data_len = url.handle(args)
		if not ret then
			only.log('E', "failed to do the " ..  os.date('%Y-%m-%d %H:%M', i) .. ' minutes round !')
			close_file()
			return nil
		end
		cnt = cnt + 1
		total_data = total_data + data_len
	end
	close_file()
	local ed_time = os.time()
	--only.log('I', 'total cost time :' .. (ed_time - st_time) .. ' seconds')
	only.log('I', string.format('write ->> %s <<- data from %s to %s, total cost time : %.6f seconds, total data size: %.4f MB', value_pre, st_date, ed_date, (ed_time - st_time), total_data/1024/1024))
	return total_data, (ed_time - st_time)
end
]]--

--==============================================================================
--==============================================================================
local data_size = 0
local time_cost = 0
local size, cost
local start_time, end_time 
if type(allow_setTime) == 'number' and allow_setTime == 1 then
	local log_date = io.open('/data/tsdb/TSDB/utils/mutil_data_integration/logs/date.log', 'r')
	local tab = {}
	local cnt = 1
	for line in log_date:lines() do
		tab[cnt] = line
		cnt = cnt + 1
	end
	for i=1, #tab do
		tab[i] = UTILS.split(tab[i], '@')
	end
	log_date:close()
	date_file = io.open('/data/tsdb/TSDB/utils/mutil_data_integration/logs/date.log', 'w+')
	
	for k, v in pairs(value_prefix) do
		cfg.ARGS['EXPIRE_TIME'] = cfg.ARGS.VALUE_EXPIRE_TIME[v]
		if v == 'URL' then
			cfg.ARGS['ALLOW_WRITE_FILE_KEY'] = 1
		else 
			cfg.ARGS['ALLOW_WRITE_FILE_KEY'] = 0
		end
		if v == 'GSENSOR' then
			cfg.ARGS['ALLOW_KEY_EXPIRE'] = 1
		else
			cfg.ARGS['ALLOW_KEY_EXPIRE'] = 0
		end
		for i=1, #tab do
			if tab[i][1] == v and tab[i][2] == 'START' then
				start_time = tab[i][3]
			elseif tab[i][1] == v and tab[i][2] == 'END' then
				end_time = tab[i][3]
			end
		end
		if start_time and end_time then
			size, cost = handle_1(v, start_time, end_time, date_file)
		else
			only.log("E", 'failed to read date time from date.log file ')
			return nil
		end
		data_size = data_size + size
		time_cost = time_cost + cost
	end
	date_file:close()
	os.execute('cp /data/tsdb/TSDB/utils/mutil_data_integration/logs/date.log /data/tsdb/int_data/backFile/')
end

--[[
else
	for k, v in pairs(value_prefix) do
		cfg.ARGS['EXPIRE_TIME'] = cfg.ARGS.VALUE_EXPIRE_TIME[v]
                if v == 'URL' then
                        cfg.ARGS['ALLOW_WRITE_FILE_KEY'] = 1
                else
                        cfg.ARGS['ALLOW_WRITE_FILE_KEY'] = 0
                end
                if v == 'GSENSOR' then
                        cfg.ARGS['ALLOW_KEY_EXPIRE'] = 1
                else
                        cfg.ARGS['ALLOW_KEY_EXPIRE'] = 0
                end
		size, cost = handle_2(v)
		data_size = data_size + size
		time_cost = time_cost + cost
	end
end
]]--

only.log('I', string.format('write all data from time %s to %s, total cost time : %.6f seconds, total data size : %.4f MB', start_time, end_time, time_cost, data_size/1024/1024))

