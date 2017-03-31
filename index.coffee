mysql = require 'mysql'
_ = require 'lodash'

module.exports = (config)->

    out = {}

    # create a connection pool with a 100 connection limit
    connection_pool = mysql.createPool
        connectionLimit: 100
        host: config.host
        user: config.user
        password: config.pass
        database: config.database

    # grab a single connection
    out.getConnection = ()->
        conn = mysql.createConnection
            host: config.host
            user: config.user
            password: config.pass
            database: config.database
        return conn

    # takes a single connection from the pool
    out.getPoolConnection = (callback)->
        connection_pool.getConnection (err, connection)->
            if err
                console.error "Failed to pull db pool-connection: ", err
                callback err
                return

            callback null, connection
            return
        return

    out.format = (query, binds)->
        return mysql.format query, binds

    out.testConnection = (callback)->
        conn = @getConnection()
        conn.query "SELECT 1 + 1 AS solution", (err, rows)->
            if err
                console.error "MySQL Test Connection Failed: ", err
                callback err
            else
                console.info "MySQL Test Connection Solution: #{rows[0].solution}"
                callback null
            conn.end()
            return
        return

    # Populates ? in queries with their respective binds and runs the query
    # Grabs a single connection from the pool, runs your query and then returns the connection to the pool
    out.execute = (query, binds, callback)->
        if _.isUndefined(callback) and _.isFunction(binds)
            callback = binds
            binds = []

        if binds.length > 0
            if _.isObject query
                query.sql = @format query.sql, binds
            else
                query = @format query, binds

    #    console.info 'db.execute', query, binds
        @getPoolConnection (err, conn)->
            if err
                console.error "DB execute() failed to pull db pool-connection:", err
                callback err
                return
            conn.query query, (err, results)->
                if err
                    console.error "DB execute() failed: #{err.message}\n\nFailed query: #{query}\n", err
                    callback err
                    return
                callback null, results

                # release the connection. this is async so who cares that it's after the callback
                conn.release()
                return
            return
        return

    # creates a safe string version of an IN() query for use in SQL queries
    # results should be later used like this "... WHERE id IN(#{in_query)"
    out.createIn = (in_list)->
    # make an array of "?" for every single element in in_list
        placeholders = _.map in_list, ->
            return '?'
        return placeholders.join(',')

    return out


