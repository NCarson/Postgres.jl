
module Postgres

    using DataFrames
    export  PostgresServer

    #####   interface stub
    # things the developer does not want you to do 
    # e.g. call select on a closed connection
    abstract DatabaseError <: Exception
    typealias Buffering Union{Val{:buffered}, Val{:streamed}}
    abstract DatabaseInterface
    abstract DatabaseConnection
    abstract DatabaseCursor{B<:Buffering}

    module Libpq
        include("libpq.jl")
        export PGconn, PGresult, ResultPtr, ConnPtr
    end

    module Types
        include("types.jl")

        export  base_types,
                AbstractPostgresType,
                PostgresType,
                PostgresDomainType,
                PostgresEnumType,
                unsafe_parse
    end

    module Results
        using DataArrays
        abstract DatabaseResult
        abstract DatabaseResultInfo
        # things the database does not like e.g. a syntax error in the SQL
        abstract DatabaseServerError <: Exception
        using ..Libpq
        using ..Types
        include("results.jl")

        export PostgresResult
    end

    using .Results
    using .Types
    using .Libpq
    include("postgres.jl")
end
