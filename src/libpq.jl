
"""
Interface to libpq - which interfaces with PostgreSQL's backend server

All functions should be considered unsafe (will segfault with bad pointers.)
Also, pointers's need their memory freed by calling the right PQ* functions.
"""
macro c(ret_type, func, arg_types, lib)
    local args_in = Any[ symbol(string('a',x)) for x in 1:length(arg_types.args) ]
    quote
        $(esc(func))($(args_in...)) = ccall( ($(string(func)), $(Expr(:quote, lib)) ),
                                            $ret_type, $arg_types, $(args_in...) )
    end
end

abstract PGconn
abstract PGresult
abstract PGcancel

typealias ResultPtr Nullable{Ptr{Libpq.PGresult}}
typealias ConnPtr Nullable{Ptr{Libpq.PGconn}}

typealias Oid UInt32
typealias ConnStatusType UInt32

const connection_status = Dict(
    (const CONNECTION_OK = 0)                   => :ok,
    (const CONNECTION_BAD = 1)                  => :bad,
    (const CONNECTION_STARTED = 2)              => :started,
    (const CONNECTION_MADE = 3)                 => :made,
    (const CONNECTION_AWAITING_RESPONSE = 4)    => :awaiting_response,
    (const CONNECTION_AUTH_OK = 5)              => :auth_ok,
    (const CONNECTION_SETENV = 6)               => :setenv,
    (const CONNECTION_SSL_STARTUP = 7)          => :ssl_startup,
    (const CONNECTION_NEEDED = 8)               => :needed,
)
typealias ExecStatusType UInt32
const exec_status = Dict(
    (const PGRES_EMPTY_QUERY = 0)               => :empty_query,
    (const PGRES_COMMAND_OK = 1)                => :command_ok,
    (const PGRES_TUPLES_OK = 2)                 => :tuples_ok,
    (const PGRES_COPY_OUT = 3)                  => :copy_out,
    (const PGRES_COPY_IN = 4)                   => :copy_in,
    (const PGRES_BAD_RESPONSE = 5)              => :bad_response,
    (const PGRES_NONFATAL_ERROR = 6)            => :nonfatal_error,
    (const PGRES_FATAL_ERROR = 7)               => :fatal_error,
    (const PGRES_COPY_BOTH = 8)                 => :copy_both,
    (const PGRES_SINGLE_TUPLE = 9)              => :single_tuple,
)

const error_field = Dict(
    :severity		            => 'S', # ERROR, WARNING etc.
    :sqlstate		            => 'C', # see apendix A in the pg man
    :message_primary            => 'M',
    :message_detail	            => 'D',
    :message_hint	            => 'H',
    :statement_position         => 'P',
    :internal_position          => 'p',
    :internal_query	            => 'q',
    :context			        => 'W',
    :schema_name		        => 's',
    :table_name		            => 't',
    :column_name		        => 'c',
    :datatype_name	            => 'd',
    :constraint_name            => 'n',
    :source_file		        => 'F',
    :source_line		        => 'L',
    :source_function            => 'R',
)

const error_state = Dict(
    "00" => :successful_completion,
    "01" => :warning,
    "02" => :no_data,
    "03" => :sql_statement_not_yet_complete,
    "08" => :connection_exception,
    "09" => :triggered_action_exception,
    "0A" => :feature_not_supported,
    "0B" => :invalid_transaction_initiation,
    "0F" => :locator_exception,
    "0L" => :invalid_grantor,
    "0P" => :invalid_role_specification,
    "0Z" => :diagnostics_exception,
    "20" => :case_not_found,
    "21" => :cardinality_violation,
    "22" => :data_exception,
    "23" => :integrity_constraint_violation,
    "24" => :invalid_cursor_state,
    "25" => :invalid_transaction_state,
    "26" => :invalid_sql_statement_name,
    "27" => :triggered_data_change_violation,
    "28" => :invalid_authorization_specification,
    "2B" => :dependent_privilege_descriptors_still_exist,
    "2D" => :invalid_transaction_termination,
    "2F" => :sql_routine_exception,
    "34" => :invalid_cursor_name,
    "38" => :external_routine_exception,
    "39" => :external_routine_invocation_exception,
    "3B" => :savepoint_exception,
    "3D" => :invalid_catalog_name,
    "3F" => :invalid_schema_name,
    "40" => :transaction_rollback,
    "42" => :syntax_error_or_access_rule_violation,
    "44" => :with_check_option_violation,
    "53" => :insufficient_resources,
    "54" => :program_limit_exceeded,
    "55" => :object_not_in_prerequisite_state,
    "57" => :operator_intervention,
    "58" => :system_error,
    "F0" => :configuration_file_error,
    "HV" => :foreign_data_wrapper_error,
    "P0" => :plpgsql_error,
    "XX" => :internal_error,
)

#### CONNECTIONS
@c Ptr{PGconn}      PQsetdbLogin (Ptr{UInt8}, Ptr{UInt8}, Ptr{UInt8}, Ptr{UInt8},
                              Ptr{UInt8}, Ptr{UInt8}, Ptr{UInt8}) libpq
@c Ptr{PGconn}      PQconnectdb (Ptr{UInt8},) libpq
@c Void             PQfinish (Ptr{PGconn},) libpq
@c Ptr{UInt8}       PQerrorMessage (Ptr{PGconn},) libpq
@c ExecStatusType   PQresultStatus (Ptr{PGresult},) libpq
@c ConnStatusType   PQstatus (Ptr{PGconn},) libpq
@c Ptr{UInt8}       PQresultErrorMessage (Ptr{PGresult},) libpq
@c Ptr{UInt8}       PQresultErrorField (Ptr{PGresult}, Cint) libpq
@c Void             PQsetNoticeReceiver (Ptr{PGconn}, Ptr{Void}, Ptr{Void}) libpq

#### EXEC COMMANDS
@c Ptr{PGresult}    PQexec (Ptr{PGconn}, Ptr{UInt8}) libpq
@c Ptr{PGresult}    PQgetResult (Ptr{PGconn},) libpq # for the end of a copy command
@c Cint             PQputCopyData (Ptr{PGconn}, Ptr{UInt8}, Cint) libpq
@c Cint             PQputCopyEnd (Ptr{PGconn}, Ptr{UInt8}) libpq
#@c Cint            PQgetCopyData (Ptr{PGconn}, Ptr{Ptr{UInt8}}, Cint) libpq

#### Results
@c Ptr{UInt8}   PQgetvalue (Ptr{PGresult}, Cint, Cint) libpq
@c Cint         PQgetisnull (Ptr{PGresult}, Cint, Cint) libpq
@c Void         PQclear (Ptr{PGresult},) libpq
# result fields
@c Cint         PQntuples (Ptr{PGresult},) libpq
@c Cint         PQnfields (Ptr{PGresult},) libpq
@c Cint         PQbinaryTuples (Ptr{PGresult},) libpq
@c Ptr{UInt8}   PQfname (Ptr{PGresult}, Cint) libpq
@c Cint         PQfnumber (Ptr{PGresult}, Ptr{UInt8}) libpq
@c Oid          PQftable (Ptr{PGresult}, Cint) libpq
@c Cint         PQftablecol (Ptr{PGresult}, Cint) libpq
@c Oid          PQftype (Ptr{PGresult}, Cint) libpq
#for update insert etc...
@c Ptr{UInt8}   PQcmdStatus (Ptr{PGresult},) libpq
@c Ptr{UInt8}   PQcmdTuples (Ptr{PGresult},) libpq

#### Escaping
@c Void         PQfreemem (Ptr{Void},) libpq
@c Ptr{UInt8}   PQescapeLiteral (Ptr{PGconn}, Ptr{UInt8}, Cint) libpq
#@c Ptr{UInt8} PQescapeIdentifier (Ptr{PGconn}, Ptr{UInt8}, Cint) libpq

#### Canceling
@c Ptr{PGcancel} PQgetCancel (Ptr{PGconn},) libpq
@c Ptr{Void}     PQfreeCancel (Ptr{PGcancel}, ) libpq
@c Cint          PQcancel (Ptr{PGcancel}, Ptr{UInt8}, Cint) libpq

#### Misc
@c Cint PQprotocolVersion (Ptr{PGconn},) libpq
@c Cint PQserverVersion (Ptr{PGconn},) libpq
@c Cint PQlibVersion (Ptr{Void},) libpq
#@c Void PQreset (Ptr{PGconn},) libpq
#@c PGTransactionStatusType PQtransactionStatus (Ptr{PGconn},) libpq
#@c Ptr{Cuchar} PQescapeByteaConn (Ptr{PGconn}, Ptr{Cuchar}, Cint, Ptr{Cint}) libpq
#@c Ptr{Cuchar} PQunescapeBytea (Ptr{Cuchar}, Ptr{Cint}) libpq
#@c Ptr{Cuchar} PQescapeBytea (Ptr{Cuchar}, Cint, Ptr{Cint}) libpq

# return query as vector of vectors with all data types as strings
function bootstrap_query(ptr::Ptr{PGconn}, query::AbstractString)

    result = PQexec(ptr, query)
    ncols = PQnfields(result)
    nrows = PQntuples(result) 
    a = Vector{Vector{UTF8String}}(nrows)
    for x in 1:nrows
        a[x] = [utf8(PQgetvalue(result, x-1, y-1)) for y in 1:ncols]
    end
    PQclear(result)
    return a
end

# not sure how to test
function interuptable_exec(ptr::Ptr{PGconn}, query::AbstractString)
    try
        res = PQexec(ptr, query)
    catch InteruptException
        cancel = PQgetCancel(ptr)
        msg = Array(UInt8, 256)
        status = PQcancel(cancel, msg, sizeof(msg))
        if status != 1
            error("cancel failed: $(bytestring(msg))")
        end
        PQfreeCancel(cancel)
        info("canceling statement due to user request")
        nothing
    end
end
