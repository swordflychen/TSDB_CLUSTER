--author : chenzutao
--date : 2014-05-26
--func: check if the data in redis is the same with data in TSDB
--
package.path = "/data/tsdb/TSDB/utils/mutil_data_integration/deploy/?.lua;/data/tsdb/TSDB/utils/mutil_data_integration/api/?.lua;/data/tsdb/TSDB/utils/mutil_data_integration/?.lua;;" .. package.path

module('CHECK_DATA', package.seeall)

local redis = require 'redis_pool_api'
local only = require 'only'
local cfg = require 'config'
local utils = require 'TSDB_UTILS'

local W_PORT = 'tsdb_w'
local R_PORT = 'tsdb_r'
local REDIS_PORT = 'tsdb_redis'

local key_prefix = cfg.ARGS['KEY_PREFIX']
local value_prefix = cfg.ARGS['VALUE_PREFIX']

--[[
local function get_redis_users(timestamp)
	
	local key = key_prefix .. ':' .. timestamp
	local status, users = redis.cmd(REDIS_PORT, 'smembers', key)
	if not status then
		only.log('E', "failed to get activeUses from redis !")
		return nil
	end
	if #users == 0 then
		only.log('E', "redis users table length is 0")
		return nil
	end
	table.sort(users)
	return users
end

local function get_redis_url(imei, timestamp)
	local key =  value_prefix .. ':' .. imei .. ':' .. timestamp
	local status, urls = redis.cmd(REDIS_PORT, 'smembers', key)
	if not status then
		only.log('E', 'get key ->' .. key .. '\'s ' .. value_prefix .. ' from redis failed !')
		return nil
	end
	if not urls or #urls == 0 then
		if value_prefix == 'URL' then
                        only.log('E', string.format('failed to get key -> %s\'s URL from redis !', key))
			return nil
		end
		return {}
	end
	table.sort(urls)
	return urls
end

local function get_TSDB_users(timestamp)
	local key = key_prefix .. ':' .. timestamp
	local status, users = redis.cmd(R_PORT, 'get', key)
	if not status then
		only.log('E', "failed to get activeUses from TSDB !")
		return nil	
	end

	if #users == 0 then
		only.log('E', "TSDB users length is 0")	
		return nil
	end
	local i, j = string.find(users, '@')
	users = string.sub(users, i+1, -1)
	local user_tab = utils.split(users, '|')
	local ret_user_tab = {}
	for _, v in pairs(user_tab) do
		if #v > 0 then
			table.insert(ret_user_tab, v)
		end
	end
	return ret_user_tab
end

local function get_TSDB_url(imei, timestamp)
	local key =  value_prefix .. ':' .. imei .. ':' .. timestamp
	local status, urls = redis.cmd(R_PORT, 'get', key)
	if not status then
		only.log('E', 'get key -> ' .. key .. '\'s ' .. value_prefix .. ' from TSDB failed !')
		return nil
	end
	if not urls or #urls == 0 then
		if value_prefix == 'URL' then
                        only.log('E', string.format('failed to get key -> %s\'s URL from redis !', key))
			return nil
		end
		return {}
	end
	local cnt = 1
	local i, j = string.find(urls, '@')
	urls = string.sub(urls, i+1, -1)
	local url = utils.split(urls, '|')
	local urls_tab = {}
	local value = ''
	local split_num
	if value_prefix == 'URL' then
		split_num = 4
	elseif value_prefix == 'GPS' then
		split_num = 12
	elseif value_prefix == 'GSENSOR' then
		split_num = 10
	end
	for i=1, #url do
		if  (i ~= 1) and (i % split_num == 1) then
			value = string.sub(value, 1, -2)
			if #value ~= 0 then
				value = string.gsub(value, '\\r\\n', '\r\n')
				table.insert(urls_tab, value)
			end
			value = ''
		end
		value = value .. url[i] .. '|'
	end
	local ret_urls_tab = {}
	for _, v in pairs(urls_tab) do
		if #v >0 then
			table.insert(ret_urls_tab, v)
		end
	end
	return ret_urls_tab
end
]]--

function handle()
	local redis_file = io.open('./redis_GPS', 'w+')
--	local tsdb_file = io.open('./logs/tsdb_url', 'w+')
	
	local key = 'GPS:406464672897566:2014082216*'
	local ok, ret = redis.cmd(REDIS_PORT, 'keys', key)
	table.sort(ret)
	for k, v in pairs(ret) do
		local ok1, result = redis.cmd(REDIS_PORT, 'smembers', v)
		redis_file:write(v .. '\n')
		for kk, vv in pairs(result) do
			local tab_vv = utils.split(vv, '|')
			if math.abs(tab_vv[1] - tab_vv[2]) > 60 then
				redis_file:write('collect time :' .. os.date('%Y-%m-%d %H:%M:%S', tab_vv[1]) .. '\n')
				redis_file:write('create  time :' .. os.date('%Y-%m-%d %H:%M:%S', tab_vv[2]) .. '\n')
			
				redis_file:write(vv .. '\n')
			end
		end
	end
	return true
end


handle()
