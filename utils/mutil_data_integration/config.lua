local socket = require 'socket'

module('config', package.seeall)

ARGS = {
	EXPIRE_TIME     = 24 * 3600, -->> key's expire time in redis( unit: sec) [[this is the default expire time, no need edit]]
	VALUE_EXPIRE_TIME = {URL = 4*3600, GPS=6*3600, GSENSOR=6*3600 }, -- every date type's expire time
	-->> mode 1:
	--XXX DEFAULT_START_TIME = '2014-06-28 00:00:00',
	-->> time form : yyyy-mm-dd HH:MM:SS
	ALLOW_SETTIME   = 1,  -->> [0,1]; 1: allow to set start and end time , 0 : deny,(default is 0)
	START_TIME      = '2014-06-29 12:00:00', -->> start time to set TSDB
	END_TIME        = '2014-06-29 23:59:59', -->> end time to set TSDB
	--XXX ALLOW_SLEEP     = 0,  -->> [0,1]; 1: sleep 10 mins between every two of reading redis, (default is 1), 0: no sleep 
	SLEEP_TIME      = 2, -->> hours to sleep when read data from redis
	NODE_TYPE	= 'SINGLE', -->> [SINGLE, CLUSTER], SINGLE: refers to using standard single machine to integrate data,  CLUSTER : refers to using computer cluster to integrate data and manage it by zookeeper 
	KEY_PREFIX	= 'ACTIVEUSER', -->> prefix substring of key write to TSDB
	VALUE_PREFIX	= {'URL','GPS','GSENSOR'}, -->>[URL, GPS, GSENSOR] prefix substring of value write to TSDB
	ALLOW_KEY_EXPIRE = 0, -->> [0,1]; 0: do not allow ACTIVEUSER key expired in redis, 1: allow key  to expire
	ALLOW_WRITE_FILE_KEY = 1, --->>[0, 1]; 0: do not allow write activeUsers to backFile, 1: write activeUsers to backFile  (default is 0)

}
