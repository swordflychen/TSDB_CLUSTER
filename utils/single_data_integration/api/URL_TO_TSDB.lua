--author : chenzutao
--date : 2014-05-09
package.path = "../deploy/?.lua;../?.lua;../lib/?.lua;;" .. package.path

module('URL_TO_TSDB', package.seeall)

local only = require 'only'
local redis_pool = require  'redis_pool_api'
local redis_short = require 'redis_short_api'
local zk_cfg = require 'cfg'
local cfg = require 'config'
local UTILS = require 'TSDB_UTILS'
local luazk = require 'libluazk'
local link = require 'link'

local W_PORT = 'tsdb_w'
local R_PORT = 'tsdb_r'
local REDIS_PORT = 'tsdb_redis'
local EXPIRE_TIME = cfg.ARGS['EXPIRE_TIME']  -->> set REDIS expire time
local ALLOW_KEY_EXPIRE = cfg.ARGS['ALLOW_KEY_EXPIRE']

local zk_info = zk_cfg.ZK_INFO['URL']
local tsdb_info = link['OWN_DIED']['redis']['tsdb_w']

local key_prefix = cfg.ARGS['KEY_PREFIX']
local value_prefix = cfg.ARGS['VALUE_PREFIX']

local function get_users(timestamp, empty_user_file)
	local key = key_prefix .. ':' .. timestamp
	local status, users = redis_pool.cmd(REDIS_PORT, 'smembers', key)
	if not status then
		only.log('E', "users table length is zero")
		return nil
	end
	if #users == 0 then
		if not empty_user_file then
			only.log('E', "empty_user_file handle is nil")
			return nil
		end
		empty_user_file:write('[BAK]' .. os.date('%Y-%m-%d_%H:%M:%S__') .. key .. '\n')
		only.log('I', " no activeUser at key -> " .. key )
	end
	return users
end

local function get_users_from_backFile(timestamp)
	local file_path = '/data/tsdb/TSDB/utils/data_integration/data/'
        local file = io.open(string.format('%sactiveUser__%s.bak', file_path, string.sub(timestamp, 1, -2)), 'r')
	local key, users
	for line in file:lines() do
		if line == string.format('ACTIVEUSER:%s', timestamp) then
			local next_line = file:read('*l')
			local i, j = string.find(next_line, '@')
			local tmp = string.sub(next_line, j+1, -1)
			users = UTILS.split(tmp, '|')
		end
	end
	file:close()
	return users
end

local function get_url_from_redis(key)
	local status, urls = redis_pool.cmd(REDIS_PORT, 'smembers', key)
	if not status then
		only.log('E', string.format('get key -> %s\'s %s from redis failed', key, value_prefix))
		return nil
	end
	if #urls == 0 then
		if value_prefix == 'URL' then
			only.log('E', string.format('failed to get key -> %s\'s URL from redis !', key))
--			return nil
		end
		return {}, 0
	end
	
	-->> keep in string format
	--e.g : dataCount*dataAttributionCount@data1|data2| ..
	table.sort(urls) 
	local attr = UTILS.split(urls[1], '|') -- to count the number of attribution in every data	
	local value = string.format('%s*%s@', #urls, #attr)
	for k , v in pairs(urls) do
		v = string.gsub(v, '\r\n', '\\r\\n')
		value = value .. v .. '|'
	end
	return value, #urls 
end		

-- single machine mode
local function set_url_to_TSDB(imei, timestamp, url_file_bak, error_file)
	local key = value_prefix .. ':' .. imei .. ':' .. timestamp
	local value, urls_len = get_url_from_redis(key)	
	
	if not error_file then
		only.log('E', "error_file file handle is nil")
		return nil
	end
	if not value then
		error_file:write(string.format( '%s__[SINGLE]_%s___%s\n', os.date('%Y%m%d%H%M%S'), value_prefix ,  key))
		return nil, nil
	end
	if #value == 0 then
		only.log('I', 'key -> ' .. key .. ' has no ' .. value_prefix)
		return true, key, 0
	end
	if not url_file_bak then
		only.log('E', "url_file_bak file handle is nil")
		return nil
	end
	local url_w = url_file_bak:write(key .. '\n' .. value .. '\n')
	local status, ret = redis_pool.cmd(W_PORT, 'set', key, value)
	if not status or not ret then
		only.log('E', "failed to set value to TSDB")
		error_file:write(string.format( '%s__[SINGLE]_%s___%s\n', os.date('%Y%m%d%H%M%S'), value_prefix ,  key))
		return nil, nil
	end
	return true, key, urls_len
end	

-- zookeeper cluster mode
local function set_url_to_TSDB_cluster(imei, timestamp, url_file_bak, error_file)
	local cluster_key = timestamp % zk_info['mod']
	local ok, data_nodes = luazk.get_write_hosts(zk_info['zkhost'], zk_info['service'], cluster_key)
	if not ok then
		only.log('E', 'set ' .. value_prefix .. ' to cluster TSDB : failed to get zookeeper write hosts !')
		return nil
	end
	if #data_nodes ~= 2 then
		only.log('E', 'set ' .. value_prefix .. ' to cluster TSDB : data node must only have two write hosts !')
		return nil
	end

	--node format:   RW:DS1:192.168.1.12:8000:9000:20130101000000:-1
	local node_1 = UTILS.split(data_nodes[1], ':')
	local node_2 = UTILS.split(data_nodes[2], ':')
	if node_1[1] ~= node_2[1] or node_1[2] ~= node_2[2] then
		if tonumber(node_1[7]) ~= -1 or node_1[7] ~= node_2[7] then
			only.log('E', 'set ' .. value_prefix .. ' to cluster TSDB : nodes have no space to write !')
			return nil
		else
			only.log('E', 'set ' .. value_prefix .. '  to cluster TSDB : two nodes must have the same write mode ! ! !')
			return nil
		end
	end
--->>> master host
	tsdb_info['host'] = node_1[3]
	tsdb_info['port'] = node_1[4]	
	
	local key =  value_prefix .. ':' .. imei .. ':' .. timestamp
	local value, urls_len = get_url_from_redis(key)	
	
	if not error_file then
		only.log('E', "error_file file handle is nil")
		return nil
	end
	if not value then
		error_file:write(string.format( '%s__[CLUSTER]_%s___%s\n', os.date('%Y%m%d%H%M%S'), value_prefix ,  key))
		return nil, nil
	end
	if #value == 0 then
		only.log('I', string.format('key -> %s has no %s !', key, value_prefix))
		return true, key, 0
	end
	if not url_file_bak then
		only.log('E', "url_file_bak file handle is nil")
		return nil
	end
	local url_w = url_file_bak:write(key .. '\n' .. value .. '\n')
	local status, ret = redis_short.cmd(W_PORT, 'set', key, value)
	if not status or not ret then
		only.log('E', "failed to set value to cluster TSDB, host : " .. tsdb_info['host'])
		error_file:write(string.format('%s__[CLUSTER host:%s]_%s___%s\n', os.date('%Y%m%d%H%M%S'), tsdb_info['host'],  value_prefix , key ))
		return nil, nil
	end
--->>> slave host
	tsdb_info['host'] = node_2[3]
	tsdb_info['port'] = node_2[4]	
	local stat, res = redis_short.cmd(W_PORT, 'set', key, value)
	if not stat or not res then
		only.log('E', "failed to set value to cluster TSDB, host : " .. tsdb_info['host'])
		error_file:write(string.format('%s__[CLUSTER host:%s]_%s___%s\n', os.date('%Y%m%d%H%M%S'), tsdb_info['host'],  value_prefix , key ))
		return nil, nil
	end
	return true, key, urls_len
end	

-- zookeeper cluster mode
local function set_user_to_TSDB_cluster(users, timestamp, user_file_bak, error_file)
	local cluster_key = timestamp % zk_info['mod']
	local ok, data_nodes = luazk.get_write_hosts(zk_info['zkhost'], zk_info['service'], cluster_key)
	if not ok then
		only.log('E', 'set activeUsers to cluster TSDB : failed to get zookeeper write hosts !')
		return nil
	end
	if #data_nodes ~= 2 then
		only.log('E', 'set activeUsers to cluster TSDB : data node must only have two write hosts !')
		return nil
	end
	-- node format : RW:DS1:192.168.1.12:8000:9000:20130101000000:-1
	local node_1 = UTILS.split(data_nodes[1], ':')
	local node_2 = UTILS.split(data_nodes[2], ':')
	if node_1[1] ~= node_2[1] or node_1[2] ~= node_2[2] then
		if tonumber(node_1[7]) ~= -1 or node_1[7] ~= node_2[7] then
			only.log('E', 'set activeUsers to cluster TSDB : nodes have no space to write !' )	
			return nil
		else
			only.log('E', 'set activeUsers to cluster TSDB : two nodes must have the same write mode !!!')
			return nil
		end
	end
--->>> master host
	tsdb_info['host'] = node_1[3]
	tsdb_info['port'] = node_1[4]
	local key = key_prefix .. ':' .. timestamp	
	table.sort(users)
	local value = string.format('%d*1@', #users)
	for k ,v in pairs(users) do
		value = value .. v .. '|'
	end
	local user_w = user_file_bak:write(key .. '\n' .. value .. '\n')
	local status, ret = redis_short.cmd(W_PORT, 'set', key, value)
	if not user_w or not status or not ret then
		only.log('E', 'failed to set user to cluster TSDB, host : ' .. tsdb_info['host'])
		error_file:write(string.format('%s__[CLUSTER host:%s]_%s___%s\n', os.date('%Y%m%d%H%M%S'), tsdb_info['host'],  key_prefix , key ))
		return nil,nil
	end
--->>> slave host
	tsdb_info['host'] = node_2[3]
	tsdb_info['port'] = node_2[4]
	local stat, res = redis_short.cmd(W_PORT, 'set', key, value)
	if not user_w or not stat or not res then
		only.log('E', 'failed to set user to cluster TSDB, host : ' .. tsdb_info['host'])
		error_file:write(string.format('%s__[CLUSTER host:%s]_%s___%s\n', os.date('%Y%m%d%H%M%S'), tsdb_info['host'],  key_prefix , key ))
		return nil,nil
	end
	return true, key
end

-->> single host mode
local function set_user_to_TSDB(users, timestamp, user_file_bak, error_file)
	local key = key_prefix .. ':' .. timestamp	
	table.sort(users)
	local value = string.format('%d*1@', #users)
	for k ,v in pairs(users) do
		value = value .. v .. '|'
	end
	if tonumber(cfg.ARGS['ALLOW_WRITE_FILE_KEY']) == 1 then
		local user_w = user_file_bak:write(key .. '\n' .. value .. '\n')
		if not user_w then
			only.log('E', string.format('failed to write %s to backFile', key_prefix))
			error_file:write(string.format('%s__[SINGLE]_%s___%s\n', os.date('%Y%m%d%H%M%S'), key_prefix , key ))
			return nil, nil
		end
	end
	local status, ret = redis_pool.cmd(W_PORT, 'set', key, value)
	if not status or not ret then
		only.log('E', 'failed to set user to TSDB')
		error_file:write(string.format('%s__[SINGLE]_%s___%s\n', os.date('%Y%m%d%H%M%S'), key_prefix , key ))
		return nil,nil
	end
	return true, key
end

function handle(args)
	local timestamp = UTILS.switch_time(args['ts']) -->传过来的时间为unix时间戳
--	local users = get_users_from_backFile(timestamp)
	local users = get_users(timestamp, args['empty_user_file'])
	if not users then
		only.log('E', "failed to get users")
		return nil
	end
	if #users == 0 then -- have no activeUser
		return true
	end
-->set uses to TSDB
	local user_set, user_key 
	if args['node_type'] == 'SINGLE' then -- single host mode
		user_set, user_key = set_user_to_TSDB(users, timestamp, args['user_file'], args['error_file'])
		
	elseif args['node_type'] == 'CLUSTER' then -- cluster host mode
		user_set, user_key = set_user_to_TSDB_cluster(users, timestamp, args['user_file'], args['error_file'])
	else
		only.log('E', 'set ' .. key_prefix .. ' to cluster TSDB : node type is wrong !')
		return nil
	end
	if not user_set then
		only.log('E', "failed to set " .. key_prefix .. " to TSDB !!!!")
		return nil
	end
-->set URL to TSDB
	local keys = {}
	local total_urls_len = 0
	if args['node_type'] == 'SINGLE' then -- single host mode
		for k, v in pairs(users) do
			local ret, url_key, urls_len = set_url_to_TSDB(v, timestamp, args['url_file'], args['error_file'])
			if not ret then
				only.log('E', 'failed to set [' .. k ..'-->' .. v .. '] \'s ' .. value_prefix .. ' to TSDB !')
				return nil
			end
			total_urls_len = total_urls_len + urls_len
			table.insert(keys, url_key)
		end
	elseif args['node_type'] == 'CLUSTER' then -- cluster host mode
		for k, v in pairs(users) do
			local ret, url_key, urls_len = set_url_to_TSDB_cluster(v, timestamp, args['url_file'], args['error_file'])
			if not ret then
				only.log('E', 'failed to set [' .. k ..'-->' .. v .. '] \'s ' .. value_prefix .. ' to TSDB !')
				return nil
			end
			total_urls_len = total_urls_len + urls_len
			table.insert(keys, url_key)
		end
	else
		only.log('E', 'set ' .. value_prefix .. ' to cluster TSDB : node type is wrong ! ! !')
		return nil
	end

	only.log('I', timestamp .. ' -->> total user ->' .. #users .. ',  total ' .. value_prefix .. '  -> ' .. total_urls_len)

	if ALLOW_KEY_EXPIRE == 1 then -- let key to expire in redis
		local ok, expir = redis_pool.cmd(REDIS_PORT, 'expire', user_key, EXPIRE_TIME)
		if not ok or expir == 0 then
			only.log('E', 'failed to set expire time for ' .. key_prefix .. ' key[' .. user_key .. ']')
			args['expire_time_file']:write(os.time('%Y%m%d%H%M%S') .. '__' .. user_key .. '\n')
			return nil
		end
	end
	for k, v in pairs(keys) do	
		-->> set expire time for URL set
		local status, exp = redis_pool.cmd(REDIS_PORT, 'expire', v, EXPIRE_TIME)
		if not status or exp == 0 then
			only.log('E', 'failed to set expire time for ' .. value_prefix .. ' key[' .. v .. ']')
			args['expire_time_file']:write(os.time('%Y%m%d%H%M%S') .. '__' .. v .. '\n')
			return nil
		end
	end	
--	print("--finished--")
	return true
end
--handle(args)
