--author : chenzutao
--date : 2014-05-09
--func: read data from recovery file and rewrite it to TSDB
--
package.path = "./deploy/?.lua;./api/?.lua;;" .. package.path

module('recovery_to_TSDB', package.seeall)

local only = require 'only'
local redis = require 'redis_pool_api'
local cfg = require 'config'

local W_PORT = 'tsdb_w'
local R_PORT = 'tsdb_r'
local REDIS_PORT = 'tsdb_redis'
local EXPIRE_TIME = cfg.ARGS['EXPIRE_TIME']
local value_prefix = cfg.ARGS['VALUE_PREFIX']
local file_prefix = '/data/tsdb/int_data/'
 
local url_file_name_rev
local url_file_rev

local user_file_name_rev
local user_file_rev

local rev_err_file
local expire_time_file

function handle()
	os.execute('mkdir -p ' .. file_prefix .. 'recoveryFile')
	os.execute('mkdir -p ' .. file_prefix .. 'errorLog')
	os.execute('mkdir -p ' .. file_prefix .. 'backFile')

	local time_file = io.open( file_prefix .. 'recoveryFile/TIME_BUILD', 'r')
	local rev_err_file = io.open(file_prefix .. 'errorLog/rev_error.log', 'a+')
	local expire_time_file = io.open(file_prefix .. 'errorLog/expireTime.log', 'a+')
	
	if not time_file then
		only.log('E', "failed to open [TIME_BUILD] file")
		return nil
	end
	local keys = {}
	-->> back URL
	for line in time_file:lines() do
		url_file_name_rev = string.format(file_prefix .. 'recoveryFile/%s__%s.rev', string.lower(value_prefix), line)
		url_file_rev = io.open(url_file_name_rev, 'r')
		if not url_file_rev then
			only.log('E', 'failed to open file [' .. url_file_name_rev .. ']')
			return nil
		end
		local cnt = 1
		local key , value
		for l in url_file_rev:lines() do
			if cnt % 2 == 1 then
				key = l
			else
				value = l
				local status, res = redis.cmd(W_PORT, 'set', key, value)
				if not status or not res then
					only.log('E', "failed to recovery to TSDB")
					rev_err_file:write('[TSDB-URL@' .. cnt .. ']' .. key .. '\n')
					return nil
				end
				table.insert(keys, key)
			end
			cnt = cnt + 1
		end
		os.execute(string.format('cp %s %sbackFile/%s__%s.bak', url_file_name_rev ,file_prefix, string.lower(value_prefix), line))

	-->> back USER 	
	
		user_file_name_rev = file_prefix .. 'recoveryFile/activeUser__' .. line .. '.rev'
		user_file_rev = io.open(user_file_name_rev, 'r')
		if not user_file_rev then
			only.log('E', 'failed to open file [' .. user_file_name_rev .. ']')
			return nil
		end
		local cnt = 1
		local key, value 
		for l in user_file_rev:lines() do
			if cnt % 2 == 1 then
				key = l
			else
				value = l
				local status, res = redis.cmd(W_PORT, 'set', key , value)
				if not status or not res then
					only.log('E', 'failed to recovery USER to TSDB')
					rev_err_file:write('[TSDB-USER@' .. cnt .. ']' .. key .. '\n')
					return nil
				end
				table.insert(keys, key)
			end
			cnt = cnt + 1
		end
		user_file_rev:close()
		os.execute('cp ' .. user_file_name_rev .. ' ' .. file_prefix .. 'backFile/activeUser__' .. line .. '.bak')
	end
	for k, v in pairs(keys) do
	-->> set expire time for activeUser key
		local status, exp = redis.cmd(REDIS_PORT, 'expire', v, EXPIRE_TIME)
		if not status or exp == 0 then
			only.log('E', "failed to set expire time for key[" .. v .. ']')
			expire_time_file:write(os.date('%Y%m%d%H%M%S') .. '__' .. v .. '\n')
			--return nil
		end	
	end
	url_file_rev:close()
	time_file:close()
	expire_time_file:close()
	--print("--finished--")
	return true
end		
handle()
				
