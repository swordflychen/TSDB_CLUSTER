--author : chenzutao
--date :   2014-07-07
--func :   get URL from TSDB, then decode URL info, and then get GPS info , GSENSOR info and set it into redis

package.path = '../deploy/?.lua;../api/?.lua;;' .. package.path

local utils = require 'TSDB_UTILS'
local redis = require 'redis_pool_api'
local only = require 'only'

local TIME_DIFF = 28800 -- 8*60*60
local GPS_POINT_UNIT = 10000000
local MATH_FLOOR = math.floor

local EW_NUMBER = {
	E = 1,
	W = -1
}

local NS_NUMBER = {
	N = 1,
	S = -1
}

local function gmttime_to_time(gmttime)
        return os.time({
			day=tonumber(string.sub(gmttime, 1, 2)), 
			month=tonumber(string.sub(gmttime, 3, 4)), 
			year=tonumber(string.sub(os.date('%Y'), 1, 2)*100) + tonumber(string.sub(gmttime, 5, 6)), 
			hour=tonumber(string.sub(gmttime, 7, 8)), 
			min=tonumber(string.sub(gmttime, 9, 10)), 
			sec=tonumber(string.sub(gmttime, 11, 12))})
end

local function convert_gps(lon, lat)
	local o_lon = string.format("%.7f",math.floor(lon/100) + lon%100/60)
	local o_lat = string.format("%.7f",math.floor(lat/100) + lat%100/60)
	return tonumber(o_lon), tonumber(o_lat)
end

local function get_users(tim)
	local key = string.format('ACTIVEUSER:%s', tim)
	local ok, ret = redis.cmd('bak_tsdb_r', 'get', key)
	if not ok then
		only.log('E','failed to get users from redis -> ' .. tim)
		return nil
	end
	if not ret or #ret == 0 then
		only.log('E', "have no users")
		return nil
	end
	local i, j = string.find(ret, '@')
	ret = string.sub(ret, j+1, -2)
	local tab = utils.split(ret, '|')
	return tab
end	

local function decode_url(str)
	local tab = utils.split(str, '|')

	local tab_url = {}
	for i=2, #tab, 4 do
		local url = utils.split(tab[i], '&')
		local table_url = {}
		
		if #url > 1 then
			for _, v in pairs(url) do
				v = utils.split(v, '=')
				table_url[v[1]] = v[2]
			end
			table.insert(tab_url, table_url)
		end
	end
	return tab_url -->>{{bizid = xxx, other = xxx, gps = xxx, imei = xxx}, {bizid = xxx ,  .....}}
end

local function decode_gps(gps, v_tab, tim, ext)
	local gps_tab = utils.split(gps, ';')
	local time
	local GPS = {}

	for i=1, #gps_tab do
		local tmp = utils.split(gps_tab[i], ',')
		if #tmp ~= 6 then
			only.log('E', string.format('failed to decode GPS: imei:%s, timestamp:%s, exact time:%s --->>>[%s]', v_tab['imei'],tim, ext, gps_tab[i]))
		end
		if tmp and #tmp == 6 then
			local val = {}

			local GMTtime = tmp[1]
			--gmtdate: 280614 --> current date: 2014-06-28
			local cur_date = os.date('%Y-%m-%d')
			local cur_gmtdate = string.format("%s%s%s",string.sub(cur_date,9,10), string.sub(cur_date,6,7), string.sub(cur_date,3,4))
			if type(GMTtime) == 'number' and string.len(GMTtime) == 11 then
				if string.sub(GMTtime,1,5) == string.sub(cur_gmtdate,2,6) then
					GMTtime = string.format("%s%s",string.sub(cur_gmtdate,1,1),GMTtime)
				end
			end
			if (type(tonumber(GMTtime)) ~= 'number') or string.len(GMTtime) ~= 12 then
				only.log('E', string.format('GMTtime error: -->>[decode_gps]->GMTtime ->%s<- type is invalid ,imei->%s<-, timestamp:%s, exact time: %s!', GMTtime, v_tab['imei'], tim, os.date('%Y-%m-%d %H:%M:%S', ext)))
				do break end
			end

			local char_lon = string.sub(tmp[2], -1)
			local char_lat = string.sub(tmp[3], -1)
			local raw_lon = tonumber(string.sub(tmp[2], 1, -2))
			local raw_lat = tonumber(string.sub(tmp[3], 1, -2))
			
			local lon, lat = convert_gps(raw_lon, raw_lat) 
			
			val['GMTtime'] = GMTtime
			val['collectTime'] = gmttime_to_time(GMTtime) + TIME_DIFF
			val['lon'] = EW_NUMBER[char_lon] * lon
			val['lat'] = NS_NUMBER[char_lat] * lat
			val['dir'] = tonumber(tmp[4])
			val['speed'] = tonumber(tmp[5])
			val['alt'] = tonumber(tmp[6])
	
			if not val['GMTtime'] 
			  or not val['collectTime'] 
			  or not val['lon'] 
			  or not val['lat'] 
			  or not val['dir'] 
			  or not val['speed'] 
			  or not val['alt'] then
				only.log('E', string.format('ERROR : imei:%s, timestamp:%s, exact time:%s --->>>[%s]', v_tab['imei'], tim, os.date('%Y-%m-%d %H:%M:%S', ext), gps_tab[i]))
			else
				table.insert(GPS, val)
			end
		end
	end
	return GPS --, time + 5 -->> GPS: {{GMTTime, lon, lat, dir, speed, alt}, {GMTTime, lon, lat .....}} 每5秒的GPS
end

local function decode_gsensor(gsensor, v_tab, tim, ext)
	local gsensor_tab = utils.split(gsensor, ';')
	local time_st, time_ed
	local GSENSOR = {}
	local gsensor_st = utils.split(gsensor_tab[#gsensor_tab], ',')  --- start time on the end
	local gsensor_ed = utils.split(gsensor_tab[1], ',')  -- end time at front
	local GMTtime_st = gsensor_st[1]
	local GMTtime_ed = gsensor_ed[1]
	
	local cur_date = os.date('%Y-%m-%d')
	local cur_gmtdate = string.format("%s%s%s",string.sub(cur_date,9,10), string.sub(cur_date,6,7), string.sub(cur_date,3,4))
	if type(GMTtime_st) == 'number' and type(GMTtime_ed) == 'number' and string.len(GMTtime) == 11 then
              if string.sub(GMTtime,1,5) == string.sub(cur_gmtdate,2,6) then
                        GMTtime_st = string.format("%s%s",string.sub(cur_gmtdate,1,1),GMTtime_st)
                        GMTtime_ed = string.format("%s%s",string.sub(cur_gmtdate,1,1),GMTtime_ed)

               end
        end

        if (type(tonumber(GMTtime_st)) ~= 'number') or type(tonumber(GMTtime_ed)) ~= 'number' or string.len(GMTtime_st) ~= 12 or string.len(GMTtime_ed) ~= 12 then
                only.log('E', string.format('GMTtime ---->>[decode_gsensor]->%s<- type is invalid, imei->%s<- , timestamp: %s, exsit time : %s !', GMTtime, v_tab['imei'], tim, os.date('%Y-%m-%d %H:%M:%S', ext)))
                return nil
        end
	local time_st = (gmttime_to_time(GMTtime_st) + TIME_DIFF)*1000
	local time_ed = (gmttime_to_time(GMTtime_ed) + TIME_DIFF)*1000
	local timestamp = (time_ed - time_st) / #gsensor_tab  -- GSENSOR 的终止时间与起始时间之差，再与GSENSOR的条数比值作为每条GSENSOR的时间增幅
	for i=1, #gsensor_tab do
		local tmp = utils.split(gsensor_tab[i], ',')
		local val = {}
		if i == 1 then
			val['collectTime'] = MATH_FLOOR(time_st/1000)
			val['millisecond'] = 0
			val['x'] = tmp[2]
			val['y'] = tmp[3]
			val['z'] = tmp[4]
		elseif i==#gsensor_tab then
			val['collectTime'] = MATH_FLOOR(time_ed/1000)
			val['millisecond'] = (time_st + (i-1)*timestamp)%1000
			val['x'] = tmp[2]
			val['y'] = tmp[3]
			val['z'] = tmp[4]
		else
			val['collectTime'] = MATH_FLOOR((time_st + i*timestamp)/1000)
			val['millisecond'] = (time_st + (i-1)*timestamp)%1000
			val['x'] = tmp[1]
			val['y'] = tmp[2]
			val['z'] = tmp[3]
		end
		table.insert(GSENSOR, val)
	end	
	return GSENSOR
end	

local function get_url(imei, tim)
	local key = string.format('URL:%s:%s', imei, tim)
	local ok, ret = redis.cmd('tsdb_r', 'get', key)
	if not ok then
		only.log('E', string.format('failed to get url --> imei:%s, time:%s', imei, tim))
		return nil
	end
	if not ret or #ret == 0 then
		only.log('E', string.format('imei:%s, time:%s --> have no url', imei, tim))
		return nil
	end
	return ret
end

function handle()
	local st_date = '2014-07-02 22:00:00'
	local ed_date = '2014-07-04 14:00:00'
	
	local unix_time_st = utils.get_time_by_date(st_date)
	local unix_time_ed = utils.get_time_by_date(ed_date)

	for i=unix_time_st, unix_time_ed, 600 do
		local tim = utils.switch_time(i)
		local users = get_users(tim)
	 	if not users then
			only.log('E', "failed to get activeUsers")
			return nil
		end

		for j=1, #users do
			local urls
			if users[j] and #users[j] > 0 then
				urls = get_url(users[j], tim)
			end

			local url_tab
			if urls and #urls>0 then
				url_tab = decode_url(urls)
			else
				only.log('E', string.format('URL is error : imei:%s, time:%s', users[j], tim))
			end

			local status, rets = redis.cmd('bak_redis_w', 'sadd', string.format('ACTIVEUSER:%s', tim), users[j])

			local gsensor_st, gsensor_ed
			local GPS, GSENSOR
			for _, v in pairs(url_tab) do
				if v['gps'] and #v['gps'] > 0 then
					GPS = decode_gps(v['gps'], v, tim, i)
				end
				if v['other'] and #v['other'] > 0 then
					GSENSOR = decode_gsensor(v['other'], v, tim, i)
				end

				local gps_info
				local gsensor_info
				if GPS then
				for l=1, #GPS do

					if not tonumber(GPS[l]['dir']) then
						print("dir is nil")
					end
					if not tonumber(GPS[l]['alt']) then
						print('alt is nil')
					end
					if not tonumber(GPS[l]['speed']) then
						print('speed is nil')
					end
					if (not tonumber(GPS[l]['dir'])) or (not tonumber(GPS[l]['alt'])) or (not tonumber(GPS[l]['speed'])) then
						only.log('E', string.format('GPS info is not complete: time : %s,key : ACTIVEUSER:%s,  imei : %s',os.date('%Y-%m-%d %H:%M:%S',i), tim, v['imei']))
					end
					--args: collectTime, createTime, imei, accountID, imsi, GMTtime, lon, lat, alt, dir, speed, tokencode
					gps_info = string.format('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s', 
								GPS[l]['collectTime'], GPS[l]['collectTime']+5, v['imei'],
								v['accountID'] or '', v['imsi'], GPS[1]['GMTtime'],
								MATH_FLOOR(GPS[l]['lon']*GPS_POINT_UNIT), MATH_FLOOR(GPS[l]['lat']*GPS_POINT_UNIT), 
								MATH_FLOOR(GPS[l]['alt']), MATH_FLOOR(GPS[l]['dir']), MATH_FLOOR(GPS[l]['speed']), v['tokencode'])
					local key = string.format('GPS:%s:%s', users[j], tim)
					local ok, ret = redis.cmd('bak_redis_w', 'sadd', key, gps_info)
					if not ok or not ret then
						only.log('E', string.format('failed to set GPS -->imei: %s, time: %s to back redis', users[j], tim))
					end
				end
				end

				if GSENSOR then
				for l=1, #GSENSOR do
					--args: collectTime, millisecond, createTime, imei, accountID, imsi, x, y, z, tokencode
					gsensor_info = string.format('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s', 
								GSENSOR[l]['collectTime'], string.format('%03d', MATH_FLOOR(GSENSOR[l]['millisecond'])),
								GSENSOR[l]['collectTime']+5, v['imei'], v['accountID'] or '', 
								v['imsi'],MATH_FLOOR(GSENSOR[l]['x']*100000), MATH_FLOOR(GSENSOR[l]['y']*100000), 
								MATH_FLOOR(GSENSOR[l]['z']*100000), v['tokencode'])
					local key = string.format('GSENSOR:%s:%s', users[j], tim)
					local ok, ret = redis.cmd('bak_redis_w', 'sadd', key, gsensor_info)
					if not ok or not ret then
						only.log('E', string.format('failed to set GSENSOR -->> imei: %s, time : %s to back redis', users[j], tim))
					end
				end
				end
			end
		end
	end
end

handle()

