--author : chenzutao
--date 	 : 2014-06-16
--func	 : compare the efficiency about getting data from  MYSQL and TSDB
package.path = '../deploy/?.lua;../api/?.lua;;' .. package.path

local socket = require 'socket'
local only = require 'only'
local link = require 'link'
local mysql = require 'mysql_pool_api'
local utils = require 'TSDB_UTILS'

local sql_fmt = {
	s_newStatus = 'select * from GPSInfo_201406 where collectTime>=%s and collectTime<=%s',
}

local function data_from_SQL()
	local st = utils.get_time_by_date('2014-06-14 00:00:00')
	local ed = utils.get_time_by_date('2014-06-15 23:59:59')
	local sql = string.format(sql_fmt['s_newStatus'] , st, ed)
        print("sql --> " .. sql)
        local ok, ret = mysql.cmd('newStatus__observer', 'select', sql)
        if not ok then
                print('failed to get data from MYSQL')
                return nil
        end
        print('data length : ' .. #ret)
end

local st = socket.gettime()
data_from_SQL()
local ed = socket.gettime()
print("cost time : " .. (ed - st))

