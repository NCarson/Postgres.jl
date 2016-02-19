
using Postgres
P = Postgres
naval = P.Types.naval

conn = connect(PostgresServer, Dict(:db=>"julia_test", :host=>"localhost"))
curs = cursor(conn)
query(curs, "select 1")

f = x -> repr(P.Types.PostgresValue(naval(x)))
exprs = [(v, f(v)) for v in values(conn.pgtypes)]
proto = x -> "select $x from generate_series(1, 1000000)"
results = []
for (t, exp) in exprs
     @time query(curs, proto(exp))
     println("$exp : $(get(curs.query_time))")
     push!(results, (get(curs.query_time).fetch_time, t))
end

for (time, kind) in results
    println("$(kind.name)\t\t\t$(round(time, 5))")
end

