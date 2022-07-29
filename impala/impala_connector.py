# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @time ：2021/6/8
# @desc : impala

from impala import dbapi


class Impala(object):
    def __init__(self, host='127.0.0.1', port=21050, user="root", password='',
                 database='default', auth_mechanism='PLAIN'):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        self.auth_mechanism = auth_mechanism
        self._conn = self.__connect()
        if self._conn:
            self._cursor_tuple = self._conn.cursor()
            self._cursor_dict = self._conn.cursor(dictify=True)

    def __connect(self):
        try:
            conn = dbapi.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                 database=self.database, auth_mechanism=self.auth_mechanism, )
        except Exception as e:
            print('连接异常: ', e)
            conn = None
        return conn

    def close(self):
        """关闭数据库连接"""
        if self._conn:
            try:
                if type(self._cursor_dict == 'object'):
                    self._cursor_dict.close()
                if type(self._cursor_tuple == 'object'):
                    self._cursor_tuple.close()
                if type(self._conn == 'object'):
                    self._conn.close()
            except Exception as e:
                print("关闭数据库连接异常", e)

    def query_tuple(self, sql):
        """查询sql,返回tuple数据集 tuple : 查询返回值是bytes"""
        query_tuple_res = ''
        try:
            # 执行sql语句
            self._cursor_tuple.execute(sql)
            query_tuple_res = self._cursor_tuple.fetchall()
        except Exception as e:
            print('发生异常:', e)
        return query_tuple_res

    def query_dict(self, sql):
        """查询sql,返回list数据集 dict : 查询返回值是bytes"""
        query_dict_res = ''
        try:
            # 执行sql语句
            self._cursor_dict.execute(sql)
            query_dict_res = self._cursor_dict.fetchall()
        except Exception as e:
            print('发生异常:', e)
        return query_dict_res

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
    impala = Impala(host='127.0.0.106', port=21050, user='root', password='123456', database='default')
    res1 = impala.query_dict('select * from test')
    res2 = impala.query_tuple('select * from test')
    impala.execute("insert into test values ('tt2')")
    impala.close()
