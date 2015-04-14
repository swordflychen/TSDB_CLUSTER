local socket = require "socket"
local luasql = require('luasql.mysql')
local only = require('only')
local link = require('link')
local APP_LINK_MYSQL_LIST = link["OWN_POOL"]["mysql"]


module('mysql_pool_api', package.seeall)

local MAX_RECNT = 10
local OWN_MYSQL_POOLS = {}


local function new_connect(sqlname)
    local nb = 0
    local mysql_info = APP_LINK_MYSQL_LIST[sqlname]
    repeat
        nb = nb + 1
        local env = assert(luasql.mysql())
        if not env then return false end

        ok,OWN_MYSQL_POOLS[sqlname] = pcall(env.connect, env, mysql_info['database'],
        mysql_info["user"], mysql_info["password"], mysql_info["host"], mysql_info["port"])
        env:close()

        if not ok then
            only.log("E", OWN_MYSQL_POOLS[sqlname])
            only.log("I", string.format('MYSQL_NAME: %s | INDEX: %s |---> Tcp:connect: FAILED!', sqlname, nb))
            OWN_MYSQL_POOLS[sqlname] = nil
        end
        if nb >= MAX_RECNT then
            return false
        end
    until OWN_MYSQL_POOLS[sqlname]
    OWN_MYSQL_POOLS[sqlname]:execute("set names utf8")
    only.log("I", string.format('MYSQL_NAME: %s | INDEX: %s |---> Tcp:connect: SUCCESS!', sqlname, nb))
    return true
end

local function fetch_pool(sqlname)
    if not APP_LINK_MYSQL_LIST[sqlname] then
        only.log("E", "NO mysql named <--> " .. sqlname)
        return false
    end
    if not OWN_MYSQL_POOLS[sqlname] then
        if not new_connect( sqlname ) then
            return false
        end
    end
    return true
end

local function flush_pool(sqlname)
    if OWN_MYSQL_POOLS[sqlname] then
        OWN_MYSQL_POOLS[sqlname]:close()
        OWN_MYSQL_POOLS[sqlname] = nil
    end
    return new_connect(sqlname)
end

-->>SELECT = "table"
-->>INSERT = "number"
-->>UPDATE = "number"
-->>AFFAIRS = "boolean"
-->>SQL_ERR = "nil"
local function mysql_api_execute(db, sql)
    local res,err = db:execute(sql)
    if not res then
        -->> when sql is error or connect is break.
        only.log("E", err)
        only.log("E", "FAIL TO DO: " .. sql)

        local break_conn = string.find(err, "LuaSQL%:%serror%sexecuting%squery%.%sMySQL%:%sMySQL%sserver%shas%sgone%saway")
        if break_conn then
            assert(false, tostring(err))
        end
        return nil
    end
    if type(res) == "number" then
        return res
    elseif type(res) == "userdata" then
        local result = {}
        local rows = res:numrows();
        for x=1,rows do
            result[x] = res:fetch({}, 'a') or {}
        end

        res:close()
        return result
    else
        only.log("E", tostring(type(res)) .. "unknow type result in mysql_api_execute(db, sql)")
        return res
    end
end

local function mysql_api_commit(db, T)
    local ok = db:setautocommit(false)
    if not ok then
        only.log("E", "affairs fail at setautocommit!")
        return false
    end

    local ret
    for i=1,#T do
        ok,ret = pcall(mysql_api_execute, db, T[i])
        if (not ok) or (not ret) then
            db:rollback()
            ok = db:setautocommit(true)
            return false
        end
        if ret == 0 then
            only.log('W', "update sql do nothing!")
        end
    end
    ret = db:commit()
    if not ret then
        only.log("E", "affairs fail at commit!")
        ok = db:setautocommit(true)
        return false
    end
    ok = db:setautocommit(true)
    if not ok then
        only.log('E', "affairs fail at set autocommit!")
        return false
    end
    return true
end

local function mysql_cmd(sqlname, cmds, ...)
    local mysql_api_list = {
        SELECT = mysql_api_execute,
        INSERT = mysql_api_execute,
        UPDATE = mysql_api_execute,
        AFFAIRS= mysql_api_commit
    }
    ----------------------------
    -- start
    ----------------------------
    cmds = string.upper(cmds)
    local begin = os.time()
    ----------------------------
    -- API
    ----------------------------
    local stat, ret
    local conn = true
    local index = 0
    repeat
        if conn then
            stat,ret = pcall(mysql_api_list[ cmds ], OWN_MYSQL_POOLS[sqlname], ...)
        end
        if not stat then
            local l = string.format("%s |--->FAILED! do reconnect . . .", cmds)
            only.log("E", l)
            conn = flush_pool(sqlname)
        end
        index = index + 1
        if index >= MAX_RECNT then
            local l = string.format("do %s rounds |--->FAILED! this request failed!", MAX_RECNT)
            only.log("E", l)
            assert(false, nil)
        end
    until stat
    ----------------------------
    -- end
    ----------------------------
    -- only.log("D", "use time :" .. (os.time() - begin))
    if ret == nil then
        assert(false, nil)
    else
        return ret
    end
end

function init( )
    for name in pairs( APP_LINK_MYSQL_LIST ) do
        local ok = new_connect( name )
        print("|-------" .. name .. " mysql pool init------->" .. (ok and " OK!" or " FAIL!"))
    end
end

function cmd(sqlname, ...)
    -- only.log('D', string.format("START MYSQL CMD |---> %f", socket.gettime()))
    -->> sqlname, cmd, sql
    -->> sqlname, {{cmd, sql}, {...}, ...}

    if not fetch_pool(sqlname) then
        return false,nil
    end

    local stat,ret,err
    if type(...) == 'table' then
        ret = {}
        for i=1,#... do
            if type((...)[i]) ~= 'table' then
                only.log("E", "error args to call mysql_api.cmd(...)")
                break
            end

            stat,ret[i] = pcall(mysql_cmd, sqlname, unpack((...)[i]))

            if not stat then err = ret[i] break end
        end
    else
        stat,ret = pcall(mysql_cmd, sqlname, ...)

        if not stat then err = ret end
    end

    -- only.log('D', string.format("END MYSQL CMD |---> %f", socket.gettime()))
    if not stat then
        only.log("E", "failed in mysql_cmd " .. tostring(err))
        return false,nil
    end
    return true,ret
end
