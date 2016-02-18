
# Postgres
## Database Interface for PostgreSQL and Julia

### Basic Usage
```julia
julia> using Postgres
julia> conn = connect(PostgresServer, Dict(:db=>"julia_test", :host=>"localhost"))
julia> #conn = connect(PostgresServer, "postgresql://localhost/julia_test")
julia> #empty strings will cause the server to use defaults.
julia> #connect(interface, user, db, host, passwd, port)
julia> #conn = connect(PostgresServer, "", "julia_test", "localhost", "", "")
julia> curs = cursor(conn)
julia> df = query(curs, "select 1 from generate_series(1,5) as s")
5x1 DataFrames.DataFrame
| Row | x1 |
|-----|----|
| 1   | 1  |
| 2   | 1  |
| 3   | 1  |
| 4   | 1  |
| 5   | 1  |
```

### Iteration

Memory management is automatic for the cursor interface.

#### Buffered (Normal) Cursor
```julia
julia> execute(curs, "select 1 from generate_series(1, 10)")
julia> for res in curs; println(res); end;
10x1 DataFrames.DataFrame
| Row | x1 |
|-----|----|
| 1   | 1  |
| 2   | 1  |
| 3   | 1  |
| 4   | 1  |
| 5   | 1  |
| 6   | 1  |
| 7   | 1  |
| 8   | 1  |
| 9   | 1  |
| 10  | 1  |
julia> for res in curs; println(res); end;
# nothing (memory already freed from server)
```

#### Streamed (Paged) Cursor
```julia
julia> streamed = cursor(conn, 3)
julia> execute(streamed, "select 1 from generate_series(1, 10)")
julia> for res in streamed; println(res); end;
3x1 DataFrames.DataFrame
| Row | x1 |
|-----|----|
| 1   | 1  |
| 2   | 1  |
| 3   | 1  |
3x1 DataFrames.DataFrame
| Row | x1 |
|-----|----|
| 1   | 1  |
| 2   | 1  |
| 3   | 1  |
3x1 DataFrames.DataFrame
| Row | x1 |
|-----|----|
| 1   | 1  |
| 2   | 1  |
| 3   | 1  |
1x1 DataFrames.DataFrame
| Row | x1 |
|-----|----|
| 1   | 1  |
0x1 DataFrames.DataFrame
```
Each iteration allocs and frees memory.

### Result Interface

Cursor must be closed (or unreachable) to release server resources.

```julia
julia> using Postgres.Results
julia> result = execute(curs, "select 1, null::int, 'HI'::text, 1.2::float8  
            from generate_series(1, 5)")
5x4{Int32, Int32, UTF8String, Float64} PostgresResult
julia> result[1,1]     # array
Nullable(1)

julia> result[1, :]    # row; also row(curs, 1)
4-element Array{Any,1}:
 Nullable(1)      
 Nullable{Int32}()
 Nullable("HI")   
 Nullable(1.2) 

# columns are a lot faster to create
julia> result[:, 1]    # columns; also column(curs, 1)
5-element DataArrays.DataArray{Int32,1}:
 1
 1
 1
 1
 1
#row iteration
julia> for row in result; println(row); end
Any[Nullable(1),Nullable{Int32}(),Nullable("HI"),Nullable(1.2)]
# ...
close(curs) # free postgres resources
```

### Transactions
```julia
julia> begin_!(curs)
julia> rollback!(curs)
julia> commit!(curs)
WARNING:  there is no transaction in progress
# transaction already ended by rollback
```

### Base Types supported as Julia Types
```julia
julia> for v in values(Postgres.Types.base_types)
            println(v)
       end

text -> UTF8String
varchar -> UTF8String
bpchar -> UTF8String
unknown -> UTF8String
bit -> BitArray{1}
varbit -> BitArray{1}
bytea -> Array{UInt8,1}
bool -> Bool
int2 -> Int16
int4 -> Int32
int8 -> Int64
float4 -> Float32
float8 -> Float64
numeric -> BigFloat
date -> Date
json -> UTF8String
jsonb -> UTF8String
```
Others supported as UTF8String.

### Extended Types
Automatically determined on connection start up.

```julia
julia> types = collect(values(conn.pgtypes))
julia> enum_test = filter(x->x.name==:enum_test, types)[1]
enum_test ∈ Set(UTF8String["happy","sad"])
# pg def:
# Schema │   Name    │ Internal name │ Size │ Elements │
#────────┼───────────┼───────────────┼──────┼──────────┼
# public │ enum_test │ enum_test     │ 4    │ happy   ↵│
#        │           │               │      │ sad      │

julia> domain_test = filter(x->x.name==:domain_test, types)[1]
(domain_test <: int4) -> Int32
# pg def:
# Schema │    Name     │  Type   │ Modifier │               Check                │
#────────┼─────────────┼─────────┼──────────┼────────────────────────────────────┼
# public │ domain_test │ integer │          │ CHECK (VALUE >= 0 AND VALUE <= 10) │
```
Enum types will use PooledDataArrays!

### Escaping
```julia
julia> user_input="1;select 'powned'"
julia> escape_value(conn, user_input)
"'1;select ''powned'''"
```

### Error Info
```julia
julia> try query(curs, "select xxx")
        catch err PostgresServerError
           println(err.info)
       end
PostgresResultInfo(
            msg:ERROR:  column "xxx" does not exist
LINE 1: select xxx
               ^
            severity:ERROR
            state:syntax_error_or_access_rule_violation
            code:42703
            primary:column "xxx" does not exist
            detail:
            hint:
            pos:8
)
```
see Appendix A. in the Postgres manual for error code/state lists.



### Copy Support
```julia
# Commands use the same interface as selects.
# Messages are passed through to Julia as you are used to seeing them in psql.
julia> println(query(curs, """
    drop table if exists s; 
    drop table if exists news; 
    create table s as select 1 as ss from generate_series(1,10)"""))
NOTICE:  table "news" does not exist, skipping
INFO: SELECT 10 10
0x0 DataFrames.DataFrame

julia> df = query(curs, "select * from s")
julia> copyto(curs, df, "s")
INFO: COPY 10 10
0x0{} PostgresResult

julia> copyto(curs, df, "news", true)
INFO: table 'news' not found in database. creating ...
INFO: CREATE TABLE 
INFO: COPY 10 10
0x0{} PostgresResult
```


### Custom Types
```julia
julia> using Postgres.Types

julia> type Point
        x::Float64
        y::Float64
       end

# find the oid (600 in this case) in the pg_type table in Postgres.
# Then instance the type.
julia> base_types[600] = PostgresType{Point}(:point, Point(0, 0))
point -> Point

# create the _in_ function from the database
julia> function Postgres.Types.unsafe_parse{T <: Point}(::PostgresType{T}, value::UTF8String)
    x, y = split(value, ",")
    x = parse(Float64, x[2:end])
    y = parse(Float64, y[1:end-1])
    Point(x, y)
end
unsafe_parse (generic function with 15 methods)

# create the _out_ function to the database
julia> Postgres.Types.PostgresValue{T <: Point}(val::T) =
    Postgres.Types.PostgresValue{T}(base_types[600], "($(val.x),$(val.y))")
Postgres.Types.PostgresValue

#reload conn so it picks up the new type
julia> close(conn)
PostgresConnection(@ 0 : not_connected)
julia> conn = connect(PostgresServer, Dict(:db=>"julia_test", :host=>"localhost"))
PostgresConnection(@ 0x0b41b818 : ok)
julia> curs = cursor(conn)
Postgres.BufferedPostgresCursor(
    PostgresConnection(@ 0x0b41b818 : ok),
    Nullable{Postgres.Results.PostgresResult}())

julia> p1 = Point(1.1, 1.1)
Point(1.1,1.1)
julia> start = repr(PostgresValue(p1))
"'(1.1,1.1)'::point"
julia> p2 = query(curs, "select $start")[1][1]
Point(1.1,1.1)
julia> p1.x == p2.x && p1.y == p2.y
true
```

### Control-C cancels the query _at_ _the_ _server_
```julia
julia> query(curs, "select 1 from generate_series(1, (10^9)::int)")
# oops; this will take forever
^CINFO: canceling statement due to user request
ERROR: PostgresError: No results to fetch
 in fetch at /home/xxx/.julia/v0.4/Postgres/src/postgres.jl:383
  in query at /home/xxx/.julia/v0.4/Postgres/src/postgres.jl:405

#no need to chase down zombie process with ps or top :) :)
```
