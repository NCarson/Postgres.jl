
#using Postgres
include("../src/Postgres.jl")
import Postgres

using Base.Test
P = Postgres

function connect(db="julia_test", host="localhost")
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
        """drop type if exists domain_test cascade;
        create domain domain_test as int
            check(value > 0 and value <= 10);""",
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


#basic conection
@test_throws P.PostgresError connect("julia_test", "/dev/null/")
conn = connect()
show(conn)
@test P.status(conn) == :ok
@test P.isopen(conn)
curs = P.cursor(conn)
show(curs)
@test P.query(curs, "select 1")[1][1] == 1
@test typeof(show(conn)) == Void
@test typeof(show(curs)) == Void

setup_db(curs)

#round trip types
for t in values(P.Types.base_types)
    show(t)
    # does not exists in PG
    if t.name == :jlunknown
        continue
    end
    println("select '$(P.Types.naval(t))'::$(t.name)")
    val = P.query(curs, "select '$(P.Types.naval(t))'::$(t.name)")[1][1]
    @test typeof(P.Types.naval(t)) == typeof(val)
    @test P.Types.naval(t) == val
end

#extended types
types = [v for v in values(conn.pgtypes)]
enum_test = filter(x->x.name==:enum_test, types)[1]
@test enum_test.enumvals == Set(UTF8String["sad","happy"])
domain_test = filter(x->x.name==:domain_test, types)[1]

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
res = P.execute(curs, "select 1, null::int from generate_series(1, 100)");
show(res)
@test !isempty(res)
@test size(res) == (100, 2)
@test size(res[:, 1]) == (100,)
@test length(res[1, :]) == 2
@test length([r for r in res]) == 100
@test length(P.Results.row(res, 1)) == 2
@test length(P.Results.column(res, 1)) == 100

P.Results.free_result!(res)
P.close(conn)

