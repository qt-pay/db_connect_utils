#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2022/3/20
# desc: ...
from database.gauss.gauss_connector import Gauss
from database.vertica.vertica_connector import Vertica
from database.oracle.oracle_connector import Oracle
from database.postgres.postgres_connector import Postgres
from database.mysql.mysql_connector import Mysql
from database.hive.hive_connector import Hive


def get_db_connector(db_type, host, port, db, username, password, options=""):
    """
    数据库入口
    :return:
    """
    db_map = {
        "gauss": Gauss,
        "vertica": Vertica,
        "oracle": Oracle,
        "postgres": Postgres,
        "mysql": Mysql,
        "hive": Hive,
    }
    db_connector = db_map[db_type](
        host=host,
        port=port,
        db=db,
        username=username,
        password=password,
        options=options,
    )

    return db_connector


if __name__ == '__main__':
    mysql = get_db_connector(
        db_type="mysql",
        host="10.0.23.106",
        port=3306,
        db="datax_test",
        username="root",
        password="123456",
    )

    res = mysql.query("select * from datax_test.test")
    print(res)
    ok = mysql.execute("insert into datax_test.test values(1,'jerry')")
    print(ok)
    mysql.close()
