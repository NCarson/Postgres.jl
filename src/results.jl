

################################################################################
#####  result info

type PostgresResultInfo <: DatabaseResultInfo
    msg::AbstractString
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
        code =      getstate(:sqlstate)
        state =     Libpq.error_state[code[1:2]] 
        primary =   getstate(:message_primary)
        detail =    getstate(:message_detail)
        hint =      getstate(:message_hint)
        pos =       getstate(:statement_position)
        PostgresResultInfo(msg, state, code, primary, detail, hint, pos)
end

function Base.show(io::IO, info::PostgresResultInfo)
    println("""PostgresResultInfo(
        \tmsg:$(strip(info.msg))
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
    println(i)
    i.state == :warning ? warn(i.msg) : nothing
    i.state == :successful_completion ? info(i.msg) : nothing
    C_NULL
end

const notice_callback_ptr = cfunction(
                                notice_callback,
                                Ptr{Void},
                                (Ptr{Void}, Ptr{PGresult},))

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

type PostgresResult
    types::Vector{AbstractPostgresType}
    colnames::Vector{UTF8String}
    nrows::Integer
    ncols::Integer
    ptr::Nullable{Ptr}

end

function PostgresResult(p::Ptr{PGresult}, types::Dict)

    s = Libpq.PQresultStatus(p)
    code = get(Libpq.exec_status, s, nothing)

    println(code)

    if code == nothing
        throw(PostgresError("unknown status $s, $msg"))

    elseif code == :fatal_error
        throw(PostgresServerError(PostgresResultInfo(p)))
        Libpq.PQclear(p)
    else
        #code == :nonfatal_error ? nonfatal_error(p) : nothing
        #code == :command_ok ? nonfatal_error(p) : nothing
        oids = [Int(Libpq.PQftype(p, col)) for col in 0:(Libpq.PQnfields(p)-1)]
        types = [get(types, oid, types[0]) for oid in oids]
        colnames = [utf8(Libpq.PQfname(p, col)) for col in 0:(Libpq.PQnfields(p)-1)]
        nrows = Libpq.PQntuples(p) 
        ncols = Libpq.PQnfields(p)
        PostgresResult(types, colnames, nrows, ncols, Nullable(p))
    end
end

function Base.show(io::IO, r::PostgresResult)
    t = join(["$(typeof(t.naval)), " for t in r.types])[1:end-2]
    print(io, "$(r.nrows)x$(r.ncols){$t} PostgresResult")
end

#Base.empty!
function free_result!(r::PostgresResult)
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

    p = (t,v) -> unsafe_parse(t, v) 
    try
        # if we have a fast ccall parser
        unsafe_parse(t, pointer("$(t.naval)"))
    catch MethodError
        #else we have to alloc a string for julia
        p = (t,v) -> unsafe_parse(t, utf8(v))
    end

    mask = Vector{Bool}(nrows)
    vals = Vector{T}(nrows)
    for row in 1:nrows
        isnull = Libpq.PQgetisnull(ptr, row-1, col-1)
        @inbounds mask[row] = isnull
        if isnull==1
            @inbounds vals[row] = t.naval
        else
            v = Libpq.PQgetvalue(ptr, row-1, col-1)
            @inbounds vals[row] = p(t, v)
        end
    end
    if isa(t, PostgresEnumType)
        PooledDataArray(vals, collect(t.enumvals), mask)
    else
        DataArray(vals, mask)
    end
end

column(result::PostgresResult, col::Int) =
    unsafe_column(get(result.ptr), col, result.nrows, result.types[col])

function row(result::PostgresResult, row::Int)
    tuple([result[row,col] for col in (1:result.ncols)] ...)
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
        return Nullable(typeof(t.naval))
    else
        v = utf8(Libpq.PQgetvalue(ptr, row, col))
        return Nullable(unsafe_parse(t, v))
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
