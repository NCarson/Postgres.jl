
library(RPostgreSQL)
drv = postgresqlInitDriver(fetch.default.rec=10**5, force.reload=T)
conn= dbConnect(drv, dbname='postgres', host='localhost')

for (i in 1:10 ) {
    print(i)
    res = dbSendQuery(conn, "select 1 from generate_series(1,1000000)")
    a  = dbFetch(res)
}
#real    0m12.914s
#user    0m4.960s
#sys     0m0.663s

#interactivly with just loop: 5.4s


