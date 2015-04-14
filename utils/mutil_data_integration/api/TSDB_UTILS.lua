--author : chenzutao
--date : 2014-05-16
package.path = "/data/tsdb/TSDB/utils/mutil_data_integration/deploy/?.lua;/data/tsdb/TSDB/utils/mutil_data_integration/?.lua;;" .. package.path

module('TSDB_UTILS', package.seeall)

local only = require 'only'
local redis = require 'redis_pool_api'
local cfg = require 'config'

local W_PORT = 'tsdb_w'
local R_PORT = 'tsdb_r'
local REDIS_PORT = 'tsdb_redis'

--------------------------------------------------------------------------------------------
-- func: split string into table format and remove char
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
---------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------
-- func: switch unix time into time format :yyyymmddHHN [N=(MM - MM%10)/10]
function switch_time(unix_time)
	local date = os.date('%Y%m%d%H', unix_time)
	local min = os.date('%M', unix_time)
	min = tonumber(min - min % 10) / 10
	return date .. min
end
---------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------
-- func : switch unix time into time format : yyyy-mm-dd HH:MM:SS
function get_date_by_time(unix_time)
	return os.time('%Y-%m-%d %H:%M:%S', unix_time)
end
---------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------
-- func: switch date format yyyy-mm-dd HH:MM:SS into unix time 
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
---------------------------------------------------------------------------------------------
