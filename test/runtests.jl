
#using Postgres
include("../src/Postgres.jl")
import Postgres

using Base.Test
P = Postgres

function dbconnect(db="julia_test", host="localhost")
    d = Dict(:db=>db, :host=>host)
    conn = nothing
    try
        conn = P.connect(P.PostgresServer, d)
    catch err
        if (ismatch(r"does not exist$", err.msg))
            run(`createdb julia_test`)
            conn = P.connect(P.PostgresServer, d)
        else
            throw(err)
        end
    end
    conn
end

function setup_db(curs::P.PostgresCursor)
    queries = [
        """drop type if exists enum_test cascade;
        create type enum_test as enum ('happy', 'sad');""",

        """drop domain if exists domain_test cascade;
        create domain domain_test as int
            check(value >= 0 and value <= 10);""",

        """select setseed(0);
        drop table if exists test;
        create table test as select 
             random() as x1 
            ,null::domain_test as x2
            ,null::enum_test as f1
            ,null::float8 as y
            from generate_series(1, 1000);
        select setseed(0);
        update test set x2 = random() * 10;
        select setseed(0);
        update test set f1 = (case
            when random() > .7 then 'happy'
            else 'sad'
        end)::enum_test;
        select setseed(0);
        update test set y =
            ((1/4. * x1) + (1/40. * x2) + 
                (case
                    when f1='happy' then .7
                    else .3
                end * 1/4) 
                + random()*1/4.);

        """
    ]
    for q in queries
        P.query(curs, q)
    end
end

function do_plsql(curs::P.PostgresCursor, cmd::AbstractString)
    P.query(curs, """
    do
    \$\$begin
    $cmd;
    end\$\$;""")
end
suppress = IOBuffer()

#basic conection
@test_throws P.PostgresError dbconnect("julia_test", "/dev/null/")
conn = dbconnect()
version = P.versioninfo(conn)
@test version[:protocol] == v"3.0.0"
print(suppress, conn)
@test P.status(conn) == :ok
@test P.isopen(conn)
curs = P.cursor(conn)
print(suppress, curs)
@test P.query(curs, "select 1")[1][1] == 1
@test typeof(show(conn)) == Void
@test typeof(show(curs)) == Void

setup_db(curs)
#transactions
P.query(curs, "drop table if exists xxx")

P.begin_(curs)
P.query(curs, "create table xxx (a int); select * from xxx;")
P.rollback(curs)
@test_throws P.Results.PostgresServerError P.query(curs, "select * from xxx")
P.commit(curs)

# close connnection so the connection will find the new user defined types.
P.close(curs)
P.close(conn)
@test P.status(conn) == :not_connected
@test_throws P.PostgresError P.query(curs, "select 1")

conn = dbconnect()
curs = P.cursor(conn)

#round trip types
for t in values(P.Types.base_types)
    print(suppress, t)
    # does not exists in PG
    if t.name == :jlunknown
        continue
    end
    val = P.query(curs, "select '$(P.Types.naval(t))'::$(t.name)")[1][1]
    @test typeof(P.Types.naval(t)) == typeof(val)
    @test P.Types.naval(t) == val
end

#extended types
types = [v for v in values(conn.pgtypes)]
enum_test = filter(x->x.name==:enum_test, types)[1]
@test enum_test.enumvals == Set(UTF8String["sad","happy"])
domain_test = filter(x->x.name==:domain_test, types)[1]
print(suppress, domain_test)
print(suppress, enum_test)

#basic query
df = P.query(curs, "select * from test")
@test size(df) == (1000,4)
@test eltype(df[1]) == Float64
@test eltype(df[2]) == Int
@test eltype(df[3]) == UTF8String

#escaping
hi ="1;select 'powned'"
P.escape_value(conn, "stuff=$hi")

#result interface

res = P.execute(curs, "select 1, null::int, 'HI'::text, 1.2::float8  
            from generate_series(1, 100)")
print(suppress, res)
@test !isempty(res)
@test size(res) == (100, 4)
@test size(res[:, 1]) == (100,)
@test length(res[1, :]) == 4
@test length([r for r in res]) == 100
@test length(P.Results.row(res, 1)) == 4
@test length(P.Results.column(res, 1)) == 100
@test_throws BoundsError res[0, :]
@test_throws BoundsError res[:, 0]
@test_throws BoundsError res[0, 0]
@test_throws BoundsError res[:, 5]
@test_throws BoundsError res[101, :]
@test_throws BoundsError res[101, 5]
for i in 1:length(res.types)
    t = res.types[i]
    tt = typeof(P.Types.naval(t))
    @test isa(res[1,i], Nullable{tt})
end
@test !isnull(res[1, 1])
@test isnull(res[1, 2])
