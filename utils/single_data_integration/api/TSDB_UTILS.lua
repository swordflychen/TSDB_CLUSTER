--author : chenzutao
--date : 2014-05-16
package.path = "../deploy/?.lua;../?.lua;;" .. package.path

module('TSDB_UTILS', package.seeall)

local only = require 'only'
local redis = require 'redis_pool_api'
local cfg = require 'config'

local W_PORT = 'tsdb_w'
local R_PORT = 'tsdb_r'
local REDIS_PORT = 'tsdb_redis'

function split(string, char)
	if not string then return nil end
	local str = string
	local tab = {}
	local idx 
	while true do
		idx = string.find(str, char)
		if idx == nil then
			tab[#tab + 1] = str
			break
		end
		tab[#tab + 1] = string.sub(str, 1, idx - 1)
		str = string.sub(str, idx + 1)
	end
	return tab
end

function switch_time(unix_time)
	local date = os.date('%Y%m%d%H', unix_time)
	local min = os.date('%M', unix_time)
	min = tonumber(min - min % 10) / 10
	return date .. min
end

function get_time_by_date(date)
	--通日期获取unix时间戳
	--date format : yyyy-mm-dd HH:MM:SS
	local sp = split(date, ' ')
	local DATE = split(sp[1], '-')
	local TIME = split(sp[2], ':')
	local time = os.time({
		year = DATE[1], month = DATE[2], day = DATE[3],
		hour = TIME[1], min = TIME[2], sec = TIME[3],
		})
	return time
end

--args :
-- timestamp, emptyUserFile, suffix
function get_users(timestamp, args)
	local key = 'ACTIVEUSER:' .. timestamp
	local status, users = redis.cmd(REDIS_PORT, 'smembers', key)
	if not status then
		only.log('E', "failed to get users from redis !")
		return nil
	end
	if #users == 0 then
		local str = string.format('%s%s__%s\n', args['suffix'], os.date('%Y-%m-%d %H:%M:%S'), key)
		args['emptyUserFile']:write(str)
		only.log('I', "no activeUser at key -> " .. key)
	end
	return users
end

function get_url_from_redis(key)
	if not key then
		only.log('E', 'key is nil !')
		return nil
	end
	local status, urls = redis.cmd(REDIS_PORT, 'hvals', key)
	if not status or #urls == 0 then
		only.log('E', 'get key -> ' .. key .. '\'s, URL from redis failed')
		return nil
	end
	return urls
end

function parsr_url(urls, timestamp)
	if not urls or #urls == 0 then
		only.log('E', "urls is nil")
		return nil
	end
	table.sort(urls)
	local attr = split(urls[1], '|')
	local value = string.format('%s*%s@', #urls, #attr)
	for k, v in pairs(urls) do
		value = value .. v .. '|'
	end
	return value
end

function parse_user(users, timestamp)
	if not users then
		only.log('E', "users is nil")
		return nil
	end
	--local key = "ACTIVEUSER:" .. timestamp
	table.sort(users)
	local value = string.format('%d*1@', #users)
	for k, v in pairs(users) do
		value = value .. v .. '|'
	end
	return key ,value
end

function write_url_to_file(imei, timestamp, args)
	local key = string.format('URL:%s:%s', imei, timestamp)
	local value = get_url_from_redis(key)
	if not value or  #value == 0 then
		only.log('E','value to file is nil' )
		return nil
	end
	local file = args['fileName']
	if file then
		local ret = file:write(key .. '\n' .. value .. '\n')
		if not ret then
			only.log('E', "failed to write URL to file")
			return nil
		end
	end
	return true, key
end

--args : errorFile, urlBakFile, imei, timestamp
function set_url_to_TSDB(imei, timestamp, args)
	local key = string.format('URL:%s:%s', args['imei'], args['timestamp'])
	local value = get_url_from_redis(key)
	
	if not value or #value == 0 then
		local str = string.format('%s@url__%s\n', os.date('%Y-%m-%d %H:%M:%S'), key)
		args['errorFile']:write(str)
		only.log('E', "url to TSDB is nil !")
		return nil, nil
	end
	--local file = args['fileName']
	local ret = write_url_to_file(imei, timestamp, args)
	if not ret then
		only.log('E', 'failed to write file')
		return nil
	end
	local status, res = redis.cmd(W_PORT, 'set', key, value)
	if not status or not res then
		only.log('E', "failed to set URL to TSDB")
		return nil, nil
	end
	return true, key
end



