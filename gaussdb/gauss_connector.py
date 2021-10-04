#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @time : 2021/10/4
# @desc : ...

import psycopg2

conn = psycopg2.connect(database="gdtaxmpp", user="di_replicate", password="Di_replicate123!", host="10.0.42.74", port="25308")

cursor = conn.cursor()

cursor.execute("DROP TABLE if exists test_conn")

cursor.execute("CREATE TABLE test_conn(id int, name text)")

cursor.execute("INSERT INTO test_conn values(1,'haha')")

cursor.execute("INSERT INTO test_conn values(2,'lala')")

cursor.execute("update test_conn set name = 'xixi'")

cursor.execute("delete from test_conn where id = 1")

conn.commit()

cursor.execute("select * from test_conn")

rows = cursor.fetchall()

for row in rows:
    print('id = ', row[0], 'name = ', row[1], '\n')

cursor.close()

conn.close()

