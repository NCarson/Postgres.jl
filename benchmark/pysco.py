
import psycopg2 as p
conn = p.connect(host='localhost', database='postgres')
curs = conn.cursor()
for i in range(10):
    print i
    curs.execute("select 1::int from generate_series(1, 1000000)")
    a = curs.fetchall()

#real    0m14.045s
#user    0m6.240s
#sys     0m0.620s

