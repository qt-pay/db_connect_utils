#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2022/3/20
# desc: ...
import settings
from jdbc_connector.gauss.gauss_connector import Gauss
from jdbc_connector.vertica.vertica_connector import Vertica
from jdbc_connector.oracle.oracle_connector import Oracle
from jdbc_connector.postgres.postgres_connector import Postgres
from jdbc_connector.mysql.mysql_connector import Mysql
from jdbc_connector.hive.hive_connector import Hive


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
        "tdpg": Postgres,
        "mysql": Mysql,
        "tdmysql": Mysql,
        "hive": Hive,
        "odps": Hive,
    }
    db_connector = db_map[db_type](
        host=host,
        port=port,
        db=db,
        username=username,
        password=password,
        options=options,
        driver=settings.DATABASE_INFO[db_type]["drive"],
        jars=settings.DATABASE_INFO[db_type]["jars"],
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
