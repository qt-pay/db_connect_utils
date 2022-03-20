#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2022/3/20
# desc: oracle
from pathlib import Path

import jaydebeapi


class Oracle(object):
    def __init__(self,
                 host="127.0.0.1",
                 port=1521,
                 db="",
                 username="root",
                 password="",
                 options="",
                 driver="oracle.jdbc.driver.OracleDriver",
                 jars="ojdbc8.jar",
                 ):
        self.username = username
        self.password = password
        self.driver = driver
        self.jars = jars
        self.url = f"jdbc:oracle:thin:@{host}:{int(port)}/{db}?{options}"
        self.conn = self.__connect()
        if self.conn:
            self._cursor = self.conn.cursor()

    def __connect(self):
        try:
            conn = jaydebeapi.connect(jclassname=self.driver, url=self.url,
                                      driver_args=[self.username, self.password], jars=f"{Path(__file__).resolve().parent}/"+self.jars)
        except Exception as e:
            print("oracle数据库连接异常:", e)
            conn = None
        return conn

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            try:
                if type(self._cursor == "object"):
                    self._cursor.close()
                if type(self.conn == "object"):
                    self.conn.close()
            except Exception as e:
                print("关闭oracle数据库连接异常", e)

    def query(self, sql):
        """查询sql,返回list数据集"""
        query_tuple_res = ""
        try:
            # 执行sql语句
            self._cursor.execute(sql)
            query_tuple_res = self._cursor.fetchall()
        except Exception as e:
            print("sql查询异常:", e)
        return query_tuple_res

    def execute(self, sql):
        """执行 增删改"""
        execute_res = False
        try:
            # 执行SQL语句
            self._cursor.execute(sql)
            # # 提交到数据库执行
            # self.conn.commit()
            execute_res = True
        except Exception as e:
            print("sql执行异常:", e)
        return execute_res


if __name__ == "__main__":
    oracle = Oracle(
        host="10.0.23.109",
        port=1521,
        db="haswj1dw",
        username="system",
        password="oracle123",
    )
    res = oracle.query("select * from auth2.sjbd_config")
    print(res)
    ok = oracle.execute("insert into auth2.sjbd_config values('1','2','jerry')")
    print(ok)
    oracle.close()
