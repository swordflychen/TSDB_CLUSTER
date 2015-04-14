
package.path = './deploy/?.lua;;' .. package.path

local redis = require('redis_pool_api')


local function get_url_from_redis(value_prefix, key)
	local status, urls = redis_pool.cmd(REDIS_PORT, 'smembers', key)
	if not status then
	only.log('E', string.format('get key -> %s\'s %s from redis failed', key, value_prefix))
	return nil
	end
	if #urls == 0 then
	if value_prefix == 'URL' then
	only.log('E', string.format('failed to get key -> %s\'s URL from redis !', key))
	return nil
	end
	return {}, 0, 0
	end

	-->> keep in string format
	--e.g : dataCount*dataAttributionCount@data1|data2| ..
--                                                                                      table.sort(urls)
	local attr = UTILS.split(urls[1], '|') -- to count the number of attribution in every data      
	local value = string.format('%s*%s@', #urls, #attr)
	local value2 = string.format('%s*%s@', #urls, #attr)
	value = string.format('%s%s', value, table.concat(urls, '|'))

	for k , v in pairs(urls) do
	v = string.gsub(v, '\r\n', '\\r\\n')
	--value = value .. v .. '|'
	value2 = string.format('%s%s|', value2, v)
	end
	if value == string.sub(value2, 1, -2) then
		print("true")
	else
		print("false")
	end
	return value, #urls, string.len(value)
end

get_url_from_redis('URL', 'URL:748982797725138:20140625000')
