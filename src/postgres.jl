

################################################################################
#####  

"""DBI interface type needed to start a connection to the Postgres server."""
type PostgresServer <: DatabaseInterface end

"""Errors that propigate in Julia."""
type PostgresError <: DatabaseError
    msg::AbstractString
end
Base.showerror(io::IO, err::PostgresError) =
    print(io, "PostgresError: $(err.msg)")


################################################################################
#####   Connection

global _last_connection = nothing
global _connection_count = 0

"""Communicater for Julia and Postgres."""
type PostgresConnection <: DatabaseConnection
    ptr::ConnPtr
    pgtypes::Dict{Int, AbstractPostgresType}
end

function Base.show(io::IO, conn::PostgresConnection) 
    if isnull(conn.ptr)
        s = :not_connected
        id = 0x00
    else
        id = @sprintf("%p", UInt(get(conn.ptr)))
        s = status(conn)
    end
    print(io, "PostgresConnection(@ $id : $s)")
end

function status(ptr::ConnPtr)
    if isnull(ptr)
        status = :not_connected
    else
        status = get(Libpq.connection_status, Libpq.PQstatus(get(ptr)), nothing)
        if status == nothing
            throw(PostgresError("unknown result status $status"))
        end
    end
    status
end
"""Returns libpq connection status given a connection."""
status(conn::PostgresConnection) = status(conn.ptr)

function require_connection(ptr::ConnPtr)
    
    if isnull(ptr)
        throw(PostgresError("database has already been disconnected"))
    end
    s = status(ptr)
    if s != :ok
        msg = utf8(Libpq.PQerrorMessage(get(ptr)))
        throw(PostgresError(msg))
    end
end

function require_connection(conn::PostgresConnection)
    require_connection(conn.ptr)
    get(conn.ptr)
end

"""Closes the connection and releases any memory that the server is holding."""
function Base.close(conn::PostgresConnection)
    if !isnull(conn.ptr)
        require_connection(conn.ptr)
        Libpq.PQfinish(get(conn.ptr))
    end
    conn.ptr = ConnPtr()
    _last_connection = nothing
    conn
end

function finalize_connect(ptr::ConnPtr)
    global _connection_count += 1
    require_connection(ptr)
    conn = PostgresConnection(ptr, base_types)
    conn.pgtypes = merge(
        base_types, 
        fetch_domain_types(conn),
        fetch_enum_types(conn))

    # last arg is id for threading etc.
    p = Ref{Cint}(_connection_count)
    Libpq.PQsetNoticeReceiver(get(ptr), Results.notice_callback_ptr, p)
    global _last_connection = conn
    finalizer(conn, close)
    conn
end

"""`connect(PostgresServer, user, db, host, passwd, port)`
Connects to a Postgres server instance.

Empty strings will cause the server to fill in default paramaters."""
function Base.connect(::Type{PostgresServer},
                      user::AbstractString,
                      db::AbstractString,
                      host::AbstractString,
                      passwd::AbstractString,
                      port::AbstractString)

    ptr = Libpq.PQsetdbLogin(host, port, C_NULL, C_NULL, db, user, passwd)
    finalize_connect(Nullable(ptr))
end

"""`connect(PostgresServer, args...)`
Convenience method to connect where missing values will be filled in by server.
"""
function Base.connect(::Type{PostgresServer}; args...)

    allowed = (:user, :db, :host, :passwd, :port)
    d = Dict(args)
    for k in keys(d)
        if !(k in allowed)
             throw(PostgresError("unknown keyword argument '$k'"))
         end
    end
    Base.connect(PostgresServer, 
        get(d, :user, ""),
        get(d, :db, ""), 
        get(d, :host, ""),
        get(d, :passwd, ""), 
        get(d, :port, "")
    )
end

"""`connect(PostgresServer, dsn)`
Uses dsn string interface to connect to the Postgres server.
"""
function Base.connect(::Type{PostgresServer}, dsn::AbstractString)
    ptr = Nullable(Libpq.PQconnectdb(dsn))
    finalize_connect(ptr)
end

"""Returns if the connection is ok."""
Base.isopen(conn::PostgresConnection) = status(conn.ptr) == :ok

"""Returns server, libpq, and libpq protocol version numbers."""
function Base.versioninfo(conn::PostgresConnection)
    function getv(v)
        v = string(v)
        a = [(i % 2 == 0 ? '.' : c) for (i, c) in enumerate(v)]
        VersionNumber(join(a))
    end

    d = Dict()
    ptr = require_connection(conn)
    d[:server] = getv(Libpq.PQserverVersion(ptr))
    d[:libpq] = getv(Libpq.PQlibVersion(C_NULL))
    d[:protocol] = getv(Libpq.PQprotocolVersion(ptr))
    d
end

################################################################################
#####  pg type bootstrap

function fetch_domain_types(conn::PostgresConnection)
    t = PostgresDomainType
    q = """
    select nspname, t.oid, t.typname, t.typbasetype 
    from pg_type t 
    join pg_namespace n 
    on t.typnamespace=n.oid 
    where typtype='d' 
    and nspname='public'"""
    d = Dict{Int, t}()
    for row in Libpq.bootstrap_query(get(conn.ptr), q)
        namespace, oid, name, parent_oid = row
        oid = parse(Int, oid)
        parent_oid = parse(Int, parent_oid)
        parent = get(conn.pgtypes, parent_oid, nothing) 
        if parent != nothing
            d[oid] = t(symbol(name), parent)
        end
    end
    d
end

function fetch_enum_types(conn::PostgresConnection)
    t = PostgresEnumType
    d = Dict{}()
    q = "select 
        enumtypid, typname, enumlabel 
        from pg_type t 
        join pg_enum e on t.oid=e.enumtypid 
        where typcategory='E' 
        order by enumtypid, enumsortorder"

    for row in Libpq.bootstrap_query(get(conn.ptr), q)
        oid, name, label = row
        oid = parse(Int, oid)
        if !haskey(d, (oid, name))
            d[(oid, name)] = Vector{UTF8String}()
        end
        d[(oid, name)] = vcat(d[(oid, name)], label)
    end
    [Int(k[1]) => 
        t(symbol(k[2]), UTF8String("∅"), Set{UTF8String}(v)) 
        for (k, v) in d]
end


################################################################################
#####  Cursor

type QueryTime
    execute_time::Float64
    fetch_time::Float64
    query::AbstractString
end

function QueryTime(t::Float64, query::AbstractString)
    QueryTime(t, 0, query)
end

function Base.show(io::IO, qt::QueryTime) 
    exc = round(qt.execute_time, 3)
    fetch = round(qt.fetch_time, 3)
    print(io, "server time: $exc\tfetch time:$fetch")
end

abstract PostgresCursor <: DatabaseCursor

"""Standard DBI type cursor that retrieves all the results at once."""
type BufferedPostgresCursor <: PostgresCursor
    conn::PostgresConnection
    result::Nullable{PostgresResult}
    query_time::Nullable{QueryTime}
    finished::Bool
end

"""Paged DBI type cursor that retrieves up to a maximum size of records.

Multiple fetches will keep returning a fixed size until an empty set is returned."""
type StreamedPostgresCursor <: PostgresCursor
    conn::PostgresConnection
    result::Nullable{PostgresResult}
    query_time::Nullable{QueryTime}
    name::AbstractString
    page_size::Int
    finished::Bool

    function StreamedPostgresCursor(conn, result, query_time, name, page_size) 
        if page_size < 0
            throw(DomainError())
        else
            new(conn, result, query_time, name, page_size, true)
        end
    end
end

function Base.show(io::IO, curs::PostgresCursor) 
    s = !isnull(curs.query_time) ? "\n\t$(get(curs.query_time))" : "" 
    p = :page_size in fieldnames(curs) ? "\n\tpage_size: $(curs.page_size)" : "" 

    print(io, "$(typeof(curs))("
        * "\n\t$(curs.conn),"
        * "\n\t$(curs.result)$p$s)")
end

"""`cursor(conn, [page_size])`
Returns a PostgresCursor given a connection.

If page_size is > 0 a StreamedPostgresCursor will be returned else a BufferedPostgresCursor.
"""
function cursor(conn::PostgresConnection, page_size=0)
    if page_size > 0
        return StreamedPostgresCursor(
            conn, 
            Nullable{PostgresResult}(),
            Nullable{QueryTime}(),
            "__julia_cursor__",
            page_size,
        )
    else
        return BufferedPostgresCursor(
            conn, 
            Nullable{PostgresResult}(),
            Nullable{QueryTime}(),
            true
        )
    end
end

#XXX this would be better to put closer to result or rename
"""`close(cursor)`
Closes and frees any results associated with the cursor."""
function Base.close(curs::PostgresCursor)
    if  (   !isnull(curs.result)
            && Libpq.PQstatus(get(curs.conn.ptr)) == :ok
        )
        close(get(curs.result))
    end
    curs.result = Nullable{PostgresResult}()
end

Base.start(::PostgresCursor) = nothing
Base.next(curs::PostgresCursor, x) = fetch(curs), nothing
Base.done(curs::PostgresCursor, x) = curs.finished

################################################################################
#####  Execute

#XXX Why do I not get notifies for these?
#    Maybe psql returns the message instead of the server?
function _transaction!(curs::PostgresCursor, cmd::AbstractString) 
    ptr = require_connection(curs.conn)
    res = Libpq.interuptable_exec(ptr, cmd)
    Results.check_status(res)
    Results.check_command(res)
    Libpq.PQclear(res)
    nothing
end

"""Begins a transaction in the session."""
begin_!(curs::PostgresCursor) = _transaction!(curs, "begin")
"""Commits a transaction in the session."""
commit!(curs::PostgresCursor) = _transaction!(curs, "commit")
"""Rollbacks a bad transaction in the session."""
rollback!(curs::PostgresCursor) = _transaction!(curs, "rollback")

function _execute(curs::BufferedPostgresCursor, sql::AbstractString)
    Libpq.interuptable_exec(get(curs.conn.ptr), sql)
end

function _execute(curs::StreamedPostgresCursor, sql::AbstractString)
    #maybe should be an error?
    if !curs.finished
        commit!(curs)
    end
    sql = "begin; declare $(curs.name) cursor for $sql"
    ptr = Libpq.interuptable_exec(get(curs.conn.ptr), sql)
    ptr
end

"""Executes a query and returns a PostgresResult."""
function execute(curs::PostgresCursor, sql::AbstractString)
    
    require_connection(curs.conn.ptr)
    if !isnull(curs.result) || !curs.finished
        close(curs)
    end
    s = time()
    ptr = _execute(curs, sql)
    #if statement was canceled by user
    if ptr == nothing
        nothing
    else
        curs.finished = false
        curs.result = PostgresResult(ptr, curs.conn.pgtypes)
        curs.query_time = Nullable{QueryTime}(QueryTime(time() - s, sql))
        finalizer(curs, close)
        get(curs.result)
    end
end


################################################################################
#####  Fetch

function _fetch!(curs::BufferedPostgresCursor)
    if isnull(curs.result)
        throw(PostgresError("No results to fetch"))
    end
    curs.finished = true
end

function _fetch!(curs::StreamedPostgresCursor)

    if curs.finished
        throw(PostgresError("No results to fetch"))
    end

    sql = "fetch forward $(curs.page_size) from $(curs.name)"
    ptr = Libpq.interuptable_exec(get(curs.conn.ptr), sql)
    curs.result = PostgresResult(ptr, curs.conn.pgtypes)
    if (get(curs.result).nrows <= 0)
        commit!(curs)
        curs.finished = true
        #close(curs)
    end
    nothing
end

"""Fetches the cursors' last result into a `DataFrame` and then frees resources."""
function Base.fetch(curs::PostgresCursor)

    _fetch!(curs)

    s = time()
    r = get(curs.result)
    columns = Vector(r.ncols)
    for col in 1:r.ncols
        columns[col] = Results.column(r, col)
    end
    names = Symbol[symbol(name=="?column?" ? "x$i" : name) 
        for (i, name) in enumerate(r.colnames)]
    df = DataFrame(columns, names)
    close(curs)
    get(curs.query_time).fetch_time = time() - s
    return df
end

################################################################################
#####  Select

"""Executes and and fetches a query."""
function query(curs::PostgresCursor, sql::AbstractString)
    execute(curs, sql)
    return fetch(curs)
end

################################################################################
#####  Escape

"""Escapes unsanitized user input."""
function escape_value(conn::PostgresConnection, s::AbstractString)
    p = require_connection(conn)
    ptr = Libpq.PQescapeLiteral(p, s, length(s))
    s = bytestring(ptr)
    Libpq.PQfreemem(ptr)
    return s
end

#FIXME
#function escape(s::AbstractString)
#    println(s)
#    escape(_last_connection, s)
#end
#
#function escape(ex::Expr)
#    for (i, arg) in enumerate(ex.args)
#        if isa(arg, Symbol)
#            ex.args[i] = :(escape(string($(esc(arg)))))
#        end
#    end
#    return ex
#end

################################################################################
#####  Copy

function writecopy(io::IO, da::DataArray)
    out = Matrix{UTF8String}(size(da)[1], size(da)[2])
    for y in 1:size(da)[1]
        for x in 1:size(da)[2]
            local v = da[y, x]
            if isa(v, AbstractString)
                v = replace(v, "\\", "\\\\")
            end
            if isna(v)
                v = "\\N"
            end
            out[y, x] = string(v)
        end
    end
    for y in 1:size(out)[1]
        writedlm(io, out[y, :])
    end
    seekstart(io)
    io
end

writecopy(io::IO, df::DataFrame) = writecopy(io, DataArray(df))

function error_message(conn::PostgresConnection)
    ptr = require_connection(conn)
    return utf8(PQerrorMessage(ptr))
end

# its copy from in PG but relatively were copying to
function copyto(curs::PostgresCursor, s::AbstractString, table::AbstractString)


    p = require_connection(curs.conn)
    res = Libpq.interuptable_exec(p, "copy $table from stdin")
    code = Results.check_status(res)
    if (code != :copy_in)
        close(res)
        throw(PostgresError("unhandled state $code"))
    end
    status = Libpq.PQputCopyData(p, s, length(s))
    status != 1 ? error(error_message(conn)) : nothing
    status = Libpq.PQputCopyEnd(p, C_NULL)
    status != 1 ? error(error_message(conn)) : nothing
    # The manual says there may be more than one query result
    # on the pipeline and we should keep getting results
    # until it returns a null pointer.
    # But, in single user copy mode once seems to be ok.
    ptr = Libpq.PQgetResult(p)
    result = PostgresResult(ptr, curs.conn.pgtypes)
    result
end

"""`copyto(curs, dataframe, tablename, [allownew])`
Fast copying of a dataframe into a Postgres table.

If table if not found and allownew is false an error will be thrown.
If table is not found and allownew is true a new table will be created."""
function copyto(curs::PostgresCursor, df::DataFrame, table::AbstractString,
                allownew=false) 

    found = table ∈ query(curs, "select tablename from pg_tables")[1]
    if !found && !allownew
        throw(PostgresError("table '$table' not found in database"))
    elseif !found
        info("table '$table' not found in database. creating ...")
        query(curs, tabledef(df, table))
    end

    buffer = readall(writecopy(IOBuffer(), df))
    copyto(curs, buffer, table)
end

function tabledef(df::DataFrame, name::AbstractString)
    types = Types.pgtypes(df)
    defs = AbstractString[]
    for col in 1:size(df)[2]
       n = df.colindex.names[col]
       kind = types[col].name
       push!(defs, "\t$n\t\t$kind")
    end
    "create table $name (\n$(join(defs, ",\n"))\n);"
end

