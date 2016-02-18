

################################################################################
#####  result info

type PostgresResultInfo <: DatabaseResultInfo
    msg::UTF8String
    severity::UTF8String
    state::Symbol
    code::UTF8String
    primary::UTF8String
    detail::UTF8String
    hint::UTF8String
    pos::UTF8String
end

function PostgresResultInfo(ptr::Ptr{PGresult})

        s = p -> p == C_NULL ? "" : utf8(p)
        getstate = sym -> s(Libpq.PQresultErrorField(ptr, Libpq.error_field[sym]))

        msg = utf8(Libpq.PQresultErrorMessage(ptr))
        severity =  getstate(:severity)
        code =      getstate(:sqlstate)
        state =     Libpq.error_state[code[1:2]] 
        primary =   getstate(:message_primary)
        detail =    getstate(:message_detail)
        hint =      getstate(:message_hint)
        pos =       getstate(:statement_position)
        PostgresResultInfo(msg, severity, state, code, primary, detail, hint, pos)
end

function Base.show(io::IO, info::PostgresResultInfo)
    println("""PostgresResultInfo(
        \tmsg:$(strip(info.msg))
        \tseverity:$(info.severity)
        \tstate:$(info.state)
        \tcode:$(info.code)
        \tprimary:$(info.primary)
        \tdetail:$(info.detail)
        \thint:$(info.hint)
        \tpos:$(info.pos)
)""")
end

function notice_callback(id::Ptr{Void},  ptr::Ptr{PGresult})
    i = PostgresResultInfo(ptr)
    if i.state in (:warning, :invalid_transaction_state)
        warn(i.msg)
    elseif i.state == :successful_completion 
        info(i.msg)
    end
    C_NULL
end

const notice_callback_ptr = cfunction(
                                notice_callback,
                                Ptr{Void},
                                (Ptr{Void}, Ptr{PGresult},))

"""Errors that propigate from the Postgres server."""
type PostgresServerError <: DatabaseServerError
    info::PostgresResultInfo
end

function Base.showerror(io::IO, err::PostgresServerError) 
    for line in split(strip(err.info.msg), "\n")
        println(io, line)
    end
end

################################################################################
#####  Result

"""Interface type to retrieve values from the server."""
type PostgresResult
    types::Vector{AbstractPostgresType}
    colnames::Vector{UTF8String}
    nrows::Integer
    ncols::Integer
    ptr::Nullable{Ptr}
end

function check_status(p::Ptr{PGresult})

    s = Libpq.PQresultStatus(p)
    code = get(Libpq.exec_status, s, nothing)

    if code == nothing
        error("unknown status $s, $msg")

    elseif code == :fatal_error
        throw(PostgresServerError(PostgresResultInfo(p)))
        Libpq.PQclear(p)
    end
    code
end

function PostgresResult(p::Ptr{PGresult}, types::Dict)

    r = nothing
    code = check_status(p)
    if code in (:tuples_ok, :empty_query, :command_ok)
        #code == :command_ok ? nonfatal_error(p) : nothing
        oids = [Int(Libpq.PQftype(p, col)) for col in 0:(Libpq.PQnfields(p)-1)]
        types = [get(types, oid, types[0]) for oid in oids]
        colnames = [utf8(Libpq.PQfname(p, col)) for col in 0:(Libpq.PQnfields(p)-1)]
        nrows = Libpq.PQntuples(p) 
        ncols = Libpq.PQnfields(p)
        r = PostgresResult(types, colnames, nrows, ncols, Nullable(p))
    else
        error("unhandled server code: $code")
    end

    if code == :command_ok
        cmd = utf8(Libpq.PQcmdStatus(p))
        num = utf8(Libpq.PQcmdTuples(p))
        info("$cmd $num")
    end
    r
end

function Base.show(io::IO, r::PostgresResult)
    t = join(["$(typeof(t.naval)), " for t in r.types])[1:end-2]
    print(io, "$(r.nrows)x$(r.ncols){$t} PostgresResult")
end

"""Frees resources held by server."""
function close(r::PostgresResult)
    if !isnull(r.ptr)
        Libpq.PQclear(get(r.ptr))
    end
    r.ptr = Nullable{ResultPtr}()
end

function unsafe_column{T}(
    ptr::Ptr{Libpq.PGresult},
    col::Int, 
    nrows::Int, 
    t::AbstractPostgresType{T}
    )

    mask = Vector{Bool}(nrows)
    vals = Vector{T}(nrows)
    for row in 1:nrows
        isnull = Libpq.PQgetisnull(ptr, row-1, col-1)
        @inbounds mask[row] = isnull
        if isnull==1
            @inbounds vals[row] = t.naval
        else
            v = Libpq.PQgetvalue(ptr, row-1, col-1)
            @inbounds vals[row] = unsafe_parse(t, v)
        end
    end
    if isa(t, PostgresEnumType)
        PooledDataArray(vals, collect(t.enumvals), mask)
    else
        DataArray(vals, mask)
    end
end

"""`column(result, col)`
Returns a DataArray of the given column number."""
function column(result::PostgresResult, col::Int)
    if !(1 <= col <= result.ncols)
        throw(BoundsError(result, (row, col)))
    end
    unsafe_column(get(result.ptr), col, result.nrows, result.types[col])
end

"""`row(result, row)`
Returns a Vector{Any} of the given row number."""
function row(result::PostgresResult, row::Int)
    #tuple([result[row,col] for col in (1:result.ncols)] ...)
    v = Vector{Any}(result.ncols)
    for col in 1:result.ncols
        v[col] = result[row, col]
    end
    v
end

function Base.getindex(result::PostgresResult, row::Int, col::Int)
    if !(1 <= row <= result.nrows && 1 <= col <= result.ncols)
        throw(BoundsError(result, (row, col)))
    end
    t = result.types[col]
    col -= 1
    row -= 1
    ptr = get(result.ptr)
    if Libpq.PQgetisnull(ptr, row, col)==1
        return Nullable{typeof(naval(t))}()
    else
        p = Libpq.PQgetvalue(ptr, row, col)
        return Nullable{typeof(naval(t))}(unsafe_parse(t, p))
    end
end

Base.getindex(result::PostgresResult, ::Colon, col::Int) =
    column(result, col)

Base.getindex(result::PostgresResult, row_::Int, ::Colon) =
    row(result, row_)

Base.length(r::PostgresResult) = r.nrows
Base.size(r::PostgresResult) = (r.nrows,r.ncols)
Base.isempty(r::PostgresResult) = (r.nrows==0 && r.ncols==0)

Base.start(r::PostgresResult) = 1
Base.endof(r::PostgresResult) = r.nrows
Base.done(r::PostgresResult, i::Int) = i >= r.nrows
Base.next(r::PostgresResult, i::Int) = (row(r, i), i+1)
#eltype
