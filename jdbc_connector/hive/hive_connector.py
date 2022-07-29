#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2022/3/20
# desc: hive
from pathlib import Path

import jaydebeapi


class Hive(object):
    def __init__(
            self,
            host="127.0.0.1",
            port=10000,
            db="",
            username="root",
            password="",
            options="",
            driver="org.apache.hive.jdbc.HiveDriver",
            jars="hive-jdbc-uber-2.6.5.0-292.jar",
    ):
        self.username = username
        self.password = password
        self.driver = driver
        self.jars = jars
        self.url = f"jdbc:hive2://{host}:{int(port)}/{db}?{options}"
        self.conn = self.__connect()
        if self.conn:
            self._cursor = self.conn.cursor()

    def __connect(self):
        try:
            conn = jaydebeapi.connect(jclassname=self.driver, url=self.url,
                                      driver_args=[self.username, self.password],
                                      jars=f"{Path(__file__).resolve().parent}/" + self.jars)
        except Exception as e:
            print("hive数据库连接异常:", e)
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
                print("关闭hive数据库连接异常", e)

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
    hive = Hive(
        host="127.0.0.106",
        port=10000,
        db="default",
        username="root",
        password="123456",
    )
    res = hive.query("select * from default.evan")
    print(res)
    # ok = hive.execute("insert into default.evan values(1,'jerry')")
    # print(ok)
    hive.close()
