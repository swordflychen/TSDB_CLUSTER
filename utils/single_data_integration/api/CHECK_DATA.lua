--author : chenzutao
--date : 2014-05-26
--func: check if the data in redis is the same with data in TSDB
--
package.path = "../deploy/?.lua;../api/?.lua;../?.lua;;" .. package.path

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

function handle(args)
	local redis_file = io.open('./logs/redis_url', 'w+')
	local tsdb_file = io.open('./logs/tsdb_url', 'w+')
	local timestamp = utils.switch_time(args['ts'])
	local redis_user = get_redis_users(timestamp)
	local tsdb_user = get_TSDB_users(timestamp)
	
	if not redis_user then
		only.log('E', "failed to get time ->" .. timestamp .. " users from redis !!!!")
		return nil
	end

	if not tsdb_user then
		only.log('E', "failed to get time ->" .. timestamp .. " users from tsdb !!!!")
		return nil
	end
--[[
	for _, v in pairs(redis_user) do
		redis_file:write(v .. '\n')
	end
	for _, v in pairs(tsdb_user) do
		tsdb_file:write(v .. '\n')
	end
]]--
	if #redis_user ~= #tsdb_user then
		print('redis_user len :' .. #redis_user)
		print('tsdb_user len :' .. #tsdb_user)
		only.log('E', "redis_user length not equare tsdb_user length")
		return nil
	end

	for i=1, #redis_user do
		if redis_user[i] ~= tsdb_user[i] then
			only.log('E' , 'redis user -> ' .. redis_user[i] .. ' != ' .. tsdb_user[i])
			return nil
		end
	end
	
	for k, v in pairs(redis_user) do
		local redis_url = get_redis_url(v, timestamp)
		local tsdb_url  = get_TSDB_url(v, timestamp)
		if not redis_url then
			print(string.format('redis url is nil : imei : %s, time : %s', v, timestamp))
			if not tsdb_url then
				print(string.format('tsdb url is nil : imei : %s, time : %s', v, timestamp))
			end
	
		elseif #redis_url ~= #tsdb_url then
			only.log('E', 'imei : ' .. v .. ' and time : ' .. timestamp .. ' ' .. value_prefix .. '  is not the same')
			return nil
		
		else
		if #redis_url > 0 then
			for i=1, #redis_url do
				if redis_url[i] ~= tsdb_url[i] then
					redis_file:write(redis_url[i] .. '\n')
					tsdb_file:write(tsdb_url[i] .. '\n')
					only.log('E', 'the ' .. i .. '\'s ' .. value_prefix .. ' is not the same !!!!!')
					return nil
				end
			end
		end
		end
	end
	return true
end
