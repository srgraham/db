mysql = require 'mysql'
_ = require 'lodash'
async = require 'async'

module.exports = (config)->

    out = {}

    # create a connection pool with a 100 connection limit
    connection_pool = mysql.createPool {
        ...config.options
        connectionLimit: config.connectionLimit ? 100
        host: config.host
        user: config.user
        password: config.pass
        database: config.database
    }


    # grab a single connection
    out.getConnection = ()->
        conn = mysql.createConnection {
            ...config.options
            host: config.host
            user: config.user
            password: config.pass
            database: config.database
        }

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

    out.insert = (table_name, row_obj, callback)->

        columns = _.keys row_obj

        query = """
            INSERT INTO #{table_name} (#{columns.join ','})
            VALUES(#{_.map columns, ->'?'})
        """

        binds = _.values row_obj

        out.execute query, binds, callback

        return

    out.update = (table_name, id_where_obj, row_obj, callback)->

        columns = _.keys row_obj
        binds = []
        sets = _.map row_obj, (val, column)->
            binds.push val
            return "#{column} = ?"

        id_wheres = _.map id_where_obj, (val, column)->
            binds.push val
            return "#{column} = ?"

        query = """
            UPDATE #{table_name}
            SET #{sets.join(',')}
            WHERE #{id_wheres.join(',')}
            LIMIT 1
        """

        out.execute query, binds, callback

        return

    out.insertOnDuplicateKeyUpdate = (table_name, row_obj, ignored_update_columns, callback)->

        columns = _.keys row_obj

        update_columns = _.without columns, ignored_update_columns...

        sets = _.map update_columns, (column)->
            return "#{column} = ?"

        query = """
            INSERT INTO #{table_name} (#{columns.join ','})
            VALUES(#{_.map columns, ->'?'})
            ON DUPLICATE KEY UPDATE
            #{sets.join(',')}
        """

        binds = _.values row_obj

        _.each update_columns, (column)->
            binds.push row_obj[column]

        if update_columns.length is 0
            err = new Error """
                no columns to update when passed to insertOnDuplicateKeyUpdate().
                Maybe theyre all ignored?
                Query:
                #{query}

                Binds:
                [#{binds}]
            """
            callback err
            return

        out.execute query, binds, callback

        return

    out.insertOnDuplicateKeyUpdateMulti = (table_name, row_obj_arr, ignore_update_columns, callback)->
        async.eachLimit row_obj_arr, 1, (row_obj, cb)->
            out.insertOnDuplicateKeyUpdate table_name, row_obj, ignore_update_columns, cb
            return
        , callback
        return

    out.insertIgnore = (table_name, row_obj, callback)->

        columns = _.keys row_obj

        query = """
            INSERT IGNORE INTO #{table_name} (#{columns.join ','})
            VALUES(#{_.map columns, ->'?'})
        """

        binds = _.values row_obj

        out.execute query, binds, callback

        return



    out.insertIgnoreMulti = (table_name, row_objs, callback)->


        insertChunk = (row_objs_chunk, cb)->

            if row_objs_chunk.length is 0
                cb null, []
                return

            columns = _.keys row_objs_chunk[0]

            placeholder_row = '(' + _.map(columns, ->'?').join(',') + ')'
            placeholder_all = _.map(row_objs_chunk, -> placeholder_row).join ','

            query = """
                INSERT IGNORE INTO #{table_name} (#{columns.join ','})
                VALUES
                  #{placeholder_all}
            """

            binds = _.flatten _.map row_objs_chunk, (row_obj)->
              return _.values row_obj

            out.execute query, binds, cb

            return

        row_chunks = _.chunk row_objs, 1000

        async.eachLimit row_chunks, 1, insertChunk, callback

        return

    # creates a safe string version of an IN() query for use in SQL queries
    # results should be later used like this "... WHERE id IN(#{in_query)"
    out.createIn = (in_list)->
    # make an array of "?" for every single element in in_list
        placeholders = _.map in_list, ->
            return '?'
        return placeholders.join(',')

    # creates a safe string version of an IN() query for use in SQL queries
    # results should be later used like this "... WHERE id IN(#{in_query)"
    out.createLikeIn = (column_name, in_list, joiner='OR')->
        if in_list.length is 0
            return "0"

        placeholders = _.map in_list, ->
            return "#{column_name} LIKE ?"
        return "( #{placeholders.join " #{joiner} "} )"

    # creates a safe string version of an IN() query for use in SQL queries
    # results should be later used like this "... WHERE id IN(#{in_query)"
    out.createRegexpIn = (column_name, in_list, joiner='OR')->
        if in_list.length is 0
            return "0"

        placeholders = _.map in_list, ->
            return "#{column_name} REGEXP ?"
        return "( #{placeholders.join " #{joiner} "} )"

    return out


