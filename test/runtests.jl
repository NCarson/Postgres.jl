
using Postgres
using Base.Test

function do_plsql(curs::P.PostgresCursor, cmd::AbstractString)
    P.query(curs, """
    do
    \$\$begin
    $cmd;
    end\$\$;""")
end

P = Postgres
conn = P.connect(P.PostgresServer, 
    Dict(:db => "postgres", :host => "localhost"))
@test P.status(conn) == :ok
@test typeof(show(conn)) == Void
curs = P.cursor(conn)
@test typeof(show(curs)) == Void
@test P.query(curs, "select 1")[1][1] == 1
