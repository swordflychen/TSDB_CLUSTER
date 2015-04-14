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
		tsdb_redis = {
			host = '172.16.11.121',
			port = 7001,
		},
		
		bak_tsdb_r = {
			host = '172.16.11.121',
			port = 7502,
		},
		-- bak_redis_w = {
		-- 	host = '172.16.11.121',
		-- 	port = 7777,
		-- },

	},
	mysql = {
		newStatus__observer = {
		host = '192.168.1.12',
		port = 3306,
		database = 'newStatus',
		user = 'observer',
		password = 'abc123'
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



