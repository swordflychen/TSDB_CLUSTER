local APP_CFG = require 'cfg'

module('only', package.seeall)

local OWN_LOGLV = {
        D = {1, "LOG_ON-OFF_DEBUG",     "[DEBUG]"       },
        I = {2, "LOG_ON-OFF_INFO",      "[INFO]"        },
        W = {3, "LOG_ON-OFF_WARN",      "[WARN]"        },
        F = {4, "LOG_ON-OFF_FAIL",      "[FAIL]"        },
        E = {5, "LOG_ON-OFF_ERROR",     "[ERROR]"       },
        S = {9, "LOG_ON_SYSTEM",        "[SYSTEM]"      },

        verbose = APP_CFG["OWN_INFO"]["LOGLV"],
}

function log(lv, msg)
	if lv ~= 'S' then
		if OWN_LOGLV[lv][1] < OWN_LOGLV['verbose'] then return end
	end
	local data =  string.format("%s %s()-->%s\n", os.date('%Y%m%d_%H%M%S'), OWN_LOGLV[ lv ][3], tostring(msg))
	f = 'access__' .. os.date('%Y%m') .. '.log'
	log_file = assert(io.open(APP_CFG['OWN_INFO']['LOG_FILE_PATH'] .. f, 'a'))
	log_file:write(data)
	log_file:close()
end


