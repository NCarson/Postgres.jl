

#import Base: connect, close, getindex, isopen, show, start, next, done, length, isempty

################################################################################
#####  

type PostgresServer <: DatabaseInterface end
type PostgresError <: DatabaseError
    msg::AbstractString
end
Base.showerror(io::IO, err::PostgresError) =
    print(io, "PostgresError: $(err.msg)")


################################################################################
#####   Connection

global _last_connection = nothing

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

function close(conn::PostgresConnection)
    if !isnull(conn.ptr)
        require_connection(conn.ptr)
        Libpq.PQfinish(get(conn.ptr))
    end
    conn.ptr = ConnPtr()
    _last_connection = nothing
    conn
end

function finalize_connect(ptr::ConnPtr)
    require_connection(ptr)
    conn = PostgresConnection(ptr, base_types)
    conn.pgtypes = merge(
        base_types, 
        fetch_domain_types(conn),
        fetch_enum_types(conn))

    # last arg is id for threading etc.
    Libpq.PQsetNoticeReceiver(get(ptr), Results.notice_callback_ptr, C_NULL)
    global _last_connection = conn
    finalizer(conn, close)
    conn
end

function Base.connect(::Type{PostgresServer},
                      user::AbstractString,
                      db::AbstractString,
                      host::AbstractString,
                      passwd::AbstractString,
                      port::AbstractString)

    ptr = Libpq.PQsetdbLogin(host, port, C_NULL, C_NULL, db, user, passwd)
    finalize_connect(Nullable(ptr))
end

function Base.connect(::Type{PostgresServer}, d::Dict)
    user = get(d, :user, "")
    db = get(d, :db, "")
    host = get(d, :host, "")
    passwd = get(d, :passwd, "")
    port = get(d, :port, "")
    Base.connect(PostgresServer, user, db, host, passwd, port)
end

function Base.connect(::Type{PostgresServer}, dsn::AbstractString)
    ptr = Nullable(Libpq.connnect(dsn))
    finalize_connect(ptr)
end

isopen(conn::PostgresConnection) = status(conn.ptr) == :ok

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

type BufferedPostgresCursor <: PostgresCursor
    conn::PostgresConnection
    result::Nullable{PostgresResult}
    query_time::Nullable{QueryTime}
    finished::Bool
end

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
function close(curs::PostgresCursor)
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

begin_(curs::PostgresCursor) = Libpq.PQexec(get(curs.conn.ptr), "begin;")
commit(curs::PostgresCursor) = Libpq.PQexec(get(curs.conn.ptr), "commit;")
rollback(curs::PostgresCursor) = Libpq.PQexec(get(curs.conn.ptr), "rollback;")

function _execute(curs::BufferedPostgresCursor, sql::AbstractString)
    Libpq.PQexec(get(curs.conn.ptr), sql)
end

function _execute(curs::StreamedPostgresCursor, sql::AbstractString)
    #maybe should be an error?
    if !curs.finished
        commit(curs)
    end
    sql = "begin; declare $(curs.name) cursor for $sql"
    ptr = Libpq.PQexec(get(curs.conn.ptr), sql)
    ptr
end

function execute(curs::PostgresCursor, sql::AbstractString)
    
    require_connection(curs.conn.ptr)
    if !isnull(curs.result) || !curs.finished
        close(curs)
    end
    s = time()
    ptr = _execute(curs, sql)
    curs.finished = false
    curs.result = PostgresResult(ptr, curs.conn.pgtypes)
    curs.query_time = Nullable{QueryTime}(QueryTime(time() - s, sql))
    finalizer(curs, close)
    get(curs.result)
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
    ptr = Libpq.PQexec(get(curs.conn.ptr), sql)
    curs.result = PostgresResult(ptr, curs.conn.pgtypes)
    if (get(curs.result).nrows <= 0)
        commit(curs)
        curs.finished = true
        #close(curs)
    end
    nothing
end

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

function query(curs::PostgresCursor, sql::AbstractString)
    execute(curs, sql)
    return fetch(curs)
end

################################################################################
#####  Escape

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
