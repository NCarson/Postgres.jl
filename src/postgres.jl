

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
    require_connection(conn.ptr)
    Libpq.PQfinish(get(conn.ptr))
    conn.ptr = ConnPtr()
    _last_connection = nothing
    nothing
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

type PostgresCursor{B<:Buffering} <: DatabaseCursor
    conn::PostgresConnection
    result::Nullable{PostgresResult}
    page_size::Int
    named_cursor::UTF8String
    execute_time::Float64
    fetch_time::Float64
    query::AbstractString
end

function Base.show(io::IO, curs::PostgresCursor) 
    if (curs.fetch_time != 0)
        exc = round(curs.execute_time, 3)
        fetch = round(curs.fetch_time, 3)
        s = "\n\tserver time: $exc\tjulia time:$fetch"
    else
        s = ""
    end
    print(io, "$(typeof(curs))(\n\t$(curs.conn),\n\t$(curs.result)$s)")
end

function cursor(conn::PostgresConnection)
    return PostgresCursor{Val{:buffered}}(
        conn, 
        Nullable{PostgresResult}(),
        0,
        "",
        0,
        0,
        ""
    )
end

function close(curs::PostgresCursor)

    if  (   !isnull(curs.result)
            && Libpq.PQstatus(get(curs.conn.ptr)) == :ok
        )
        close(get(curs.result))
    end
    curs.result = Nullable{PostgresResult}()
end


################################################################################
#####  Execute

# begin; DECLARE liahona CURSOR FOR SELECT 1 from generate_series(1,100);
# fetch forward 10000 from liahona;
# commit

function execute(curs::PostgresCursor, sql::AbstractString)

    curs.execute_time = 0
    curs.fetch_time = 0
    curs.query = sql

    require_connection(curs.conn.ptr)
    if !isnull(curs.result)
        close(curs)
    end
    s = time()
    ptr = Libpq.PQexec(get(curs.conn.ptr), sql)
    curs.result = PostgresResult(ptr, curs.conn.pgtypes)
    finalizer(curs, close)
    curs.execute_time = time() - s
    get(curs.result)
end

################################################################################
#####  Fetch

function fetch(curs::PostgresCursor)

    if isnull(curs.result)
        throw(PostgresError("No results to fetch"))
    end

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
    curs.fetch_time = time() - s
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
