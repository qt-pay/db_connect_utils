# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @time ：2021/6/8
# @desc : hive

# from impala import dbapi
import pymysql
from pyhive import hive


class Hive(object):
    def __init__(self, host='127.0.0.1', port=10000, username="root", password='',
                 database='default', auth='LDAP'):
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.database = database
        self.auth = auth
        self._conn = self.__connect()
        if self._conn:
            self._cursor_tuple = self._conn.cursor()

    def __connect(self):
        try:
            conn = hive.connect(host=self.host, port=self.port, username=self.username, password=self.password,
                                database=self.database, auth=self.auth, )
        except Exception as e:
            print('连接异常: ', e)
            conn = None
        return conn

    def close(self):
        """关闭数据库连接"""
        if self._conn:
            try:
                if type(self._cursor_tuple == 'object'):
                    self._cursor_tuple.close()
                if type(self._conn == 'object'):
                    self._conn.close()
            except Exception as e:
                print("关闭数据库连接异常", e)

    def query_tuple(self, sql):
        """查询sql,返回tuple数据集 tuple : 查询返回值是string"""
        query_tuple_res = ''
        try:
            # 执行sql语句
            self._cursor_tuple.execute(sql)
            query_tuple_res = self._cursor_tuple.fetchall()
        except Exception as e:
            print('发生异常:', e)
        return query_tuple_res

    def execute(self, sql):
        """执行 增删改"""
        execute_res = False
        try:
            # 执行SQL语句
            self._cursor_tuple.execute(sql)
            # 提交到数据库执行
            self._conn.commit()
            execute_res = True
        except Exception as e:
            print('发生异常:', e)
            # 发生错误时回滚
            # self._conn.rollback()
        return execute_res


if __name__ == '__main__':
    hive = Hive(host='10.0.23.106', port=10000, username='root', password='123456', database='default')
    res1 = hive.query_tuple('select * from test')
    print(res1)
    hive.execute("insert into test values ('tt2','23')")
    hive.close()
