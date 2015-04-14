module(...)

OWN_LINK = {

}

OWN_INIT = {

}

OWN_INFO = {
    LOGLV = 0,
    POOLS = true,
    LOG_FILE_PATH = '/data/tsdb/TSDB/utils/mutil_data_integration/logs/'
}

-- zookeeper  configuration
ZK_INFO = {
        URL = {
                zkhost  = '192.168.11.95:2181,192.168.11.95:2182,192.168.11.95:2183,192.168.11.95:2184,192.168.11.95:2185',
                service = 'URL',
                mod   = 8192,
                --mod     = 1,
              },

}

