module("link", package.seeall)

OWN_POOL = {
	redis = {
		tsdb_w = {
			host = '172.16.11.121',
			port = 7501,
		},
		tsdb_r = {
			host = '172.16.11.121',
			port = 7502,
		},
--[[
		tsdb_redis = {
			host = '172.16.11.121',
			port = 7001,
		},
]]--
		tsdb_redis = {
			host = '172.16.11.121',
			port = 7777,
		},

	},
}


OWN_DIED = {
	redis = {
		tsdb_w = {
			host = '172.16.11.121',
			port = 7501,
		},
		tsdb_r = {
			host = '172.16.11.121',
			port = 7502,
		},
		tsdb_redis = {
			host = '172.16.11.121',
			port = 7001,
		},


	},

	mysql = {

	},

	api = {

	},

	weibo = {

	},
  }



