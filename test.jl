
include("src/Postgres.jl")
P = Postgres
conn = P.connect(P.PostgresServer, Dict(:db => "equibase", :host => "localhost"))
curs = P.cursor(conn)
P.execute(curs, "select 1, 2, 'cat', 1.2::float4, null from generate_series(1,3)")

function do_plsql(curs::P.PostgresCursor, cmd::AbstractString)
    P.query(curs, """
    do
    \$\$begin
    $cmd;
    end\$\$;""")
end

