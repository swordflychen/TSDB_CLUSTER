--author : chenzutao
--2014-05-16
--
package.path = "../deploy/?.lua;../?.lua;;" .. package.path

module('WRITE_RECOVERY', package.seeall)

local only = require 'only'
local redis = require 'redis_pool_api'
local cfg = require 'config'
local UTILS = require 'TSDB_UTILS'

local W_PORT = 'tsdb_w'
local R_PORT = 'tsdb_r'
local REDIS_PORT = 'tsdb_redis'
local EXPIRE_TIME = cfg.ARGS['EXPIRE_TIME']

local key_prefix = cfg.ARGS['KEY_PREFIX']
local value_prefix = cfg.ARGS['VALUE_PREFIX']

local function get_users(timestamp, empty_user_file)
	local key = key_prefix .. ':' .. timestamp
	local status, users = redis.cmd(REDIS_PORT, 'smembers', key)
	if not status then
		only.log('E', 'failed to get users !')
		return nil
	end
	if #users == 0 then
		if not empty_user_file then
			only.log('E', "empty_user_file file handle is nil")
			return nil
		end
		empty_user_file:write('[REV]' .. os.date('%Y-%m-%d_%H:%M:%S__') .. key .. '\n')
		only.log('I', " no activeUser at key -> " .. key)
	end
	only.log('I', timestamp .. ' -> user mount -> ' .. #users)
	return users
end

local function get_url_from_redis(key)
	local status, urls = redis.cmd(REDIS_PORT, 'smembers', key)
	if not  status or #urls == 0 then
		only.log('E', 'get key ->' .. key .. '\'s ' .. value_prefix .. ' from redis failed')
		return nil
	end
--	only.log('I', 'key -> '.. key .. ' -> url mount -> ' .. #urls)
	-->> keep value in string format
	table.sort(urls)
	local attr = UTILS.split(urls[1], '|')
	local value = string.format('%s*%s@', #urls, #attr)
	for k, v in pairs(urls) do
		v = string.gsub(v, '\r\n', '\\r\\n')
		value = value .. v .. '|'
	end
	return value
end

local function write_url_to_recoveryFile(imei, timestamp, url_file_rev)
	-->> wtite URL to file
	local key = value_prefix .. ':' .. imei .. ':' .. timestamp
	local value = get_url_from_redis(key)
	
	if not value or #value == 0 then
		only.log('E', 'value length is 0')
		return nil
	end
	if not url_file_rev then
		only.log('E', "url_file_rev file handle is nil")
		return nil
	end	
	local ret = url_file_rev:write(key .. '\n' .. value .. '\n')
	if not ret then
		only.log('E', "failed to write URL to recovery file")
		return nil
	end
	return true, key
end

local function write_user_to_recoveryFile(users, timestamp, user_file_rev)
	-->> write USER to file
	local key = key_prefix .. ":" .. timestamp
	-->> keep users in string format
	table.sort(users)
	local value = string.format('%d*1@', #users)
	for k, v in pairs(users) do
		value = value .. v .. '|'
	end
	if not user_file_rev then
		only.log('E', 'user_file_rev file handle is nil')
		return nil
	end
	local ret = user_file_rev:write(key .. '\n' .. value .. '\n')
	if not ret then
		only.log('E', 'failed to write USER to recovery file')
		return nil
	end
	return true, key
end

local function switch_time(unix_time)
	local date = os.date('%Y%m%d%H', unix_time)
	local min = os.date('%M', unix_time)
	min = tonumber(min - min % 10) / 10
	return date .. min
end

function handle(args)
	local timestamp = switch_time(args['ts'])
	local users = get_users(timestamp, args['empty_user_file'])
	if not users then
		only.log('E', 'failed to get USERS !')
		return nil
	end
	if #users == 0 then
		return true
	end
	local user_w, user_key = write_user_to_recoveryFile(users, timestamp, args['user_file'])
	if not user_w then
		only.log('E', "failed to write USERS to file")
		return nil
	end
	
	local keys = {}
	for k ,v in pairs(users) do
		local ret, url_key = write_url_to_recoveryFile(v, timestamp, args['url_file'])
		if not ret then
			only.log('E', 'failed to wtite URL to file')
			return nil
		end
		table.insert(keys, url_key)
	end
	for k, v in pairs(keys) do
		-->>set expire time for url key
		local status, exp = redis.cmd(REDIS_PORT, 'expire', v, EXPIRE_TIME)
		if not status or exp == 0 then
			only.log('E', "failed to set expire time for key ->" .. v)
			args['expire_time_file']:write(os.date('%Y%m%d%H%M%S') .. '__' .. v .. '\n')
		end
	end
	-->>set expire time for user key
	local status, exp = redis.cmd(REDIS_PORT, 'expire', user_key , EXPIRE_TIME)
	if not status or exp == 0 then
		only.log('E', "failed to set expire time for key ->" .. user_key)
		args['expire_time_file']:write(os.date('%Y%m%d%H%M%S') .. '__' .. user_key .. '\n')
	end
	return true
end

--handle(args)

