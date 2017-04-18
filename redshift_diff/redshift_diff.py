"""
Redshift diff

Used to determine if 2 redshift databases are setup the exact same

Usage:
    redshift_diff.py <db1> <db2>

"""

from docopt import docopt
import sqlalchemy


if __name__ == '__main__':
    arguments = docopt(__doc__)
    db1s = arguments['<db1>']
    db2s = arguments['<db2>']

    sql = "select * from pg_table_def where schemaname ='public';"
    conn = sqlalchemy.create_engine(db1s)
    db = {}
    pair1 = (conn, db)
    conn2 = sqlalchemy.create_engine(db2s)
    db2 = {}
    pair2 = (conn2, db2)
    pr = conn.execute(sql)

    for connection, dict_object in [pair1,pair2]:
        pr = connection.execute(sql)
        for row in pr:
            t = row['tablename']
            c = row['column']
            data = {
                'type': row['type'],
                'encoding': row['encoding'],
                'diskey': row['distkey'],
                'sortkey': row['sortkey'],
                'notnull': row['notnull']
            }
            dict_object[t + ' + ' + c] = data

    l_set = set(db.keys()) - set(db2.keys()) 
    r_set = set(db2.keys()) - set(db.keys()) 
    print("these table + columns are not in db2")
    for t in sorted(r_set):
        print t
    print("-----------------------------")
    print("these table + columns are not in db1")
    for t in sorted(l_set):
        print t
    print("-----------------------------")


    print("Differences")
    for k in set.intersection(set(db.keys()), set(db2.keys())):
        #print(k)
        if db[k] != db2[k]:
            print("-----------------------------")
            print("table + column : ", k)
            print("db1:" , db[k])
            print("db2:" , db2[k])
            print("-----------------------------")
