# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @time ：2021/5/26
# @desc : Oracle 连接
import cx_Oracle


class Oracle(object):
    def __init__(self, host='127.0.0.1', port=1521, user='root', password='', db='test', charset='utf8'):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset
        self._conn = self.__connect()
        if self._conn:
            self._cursor_tuple = self._conn.cursor()

    def __connect(self):
        try:
            conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.db))
        except Exception as e:
            print("连接异常:", e)
            conn = None
        return conn

    def close(self):
        """关闭数据库连接"""
        if self._conn:
            try:
                if type(self._cursor_tuple == 'object'):
                    self._cursor_tuple.close()
                if type(self._cursor_dict == 'object'):
                    self._cursor_dict.close()
                if type(self._conn == 'object'):
                    self._conn.close()
            except Exception as e:
                print("关闭数据库连接异常", e)

    def query_tuple(self, sql):
        """查询sql,返回tuple数据集 tuple"""
        query_tuple_res = ''
        try:
            # 执行sql语句
            self._cursor_tuple.execute(sql)
            query_tuple_res = self._cursor_tuple.fetchall()
        except Exception as e:
            print('发生异常:', e)
        return query_tuple_res

    @staticmethod
    def make_dict_factory(cursor):
        columnNames = [d[0] for d in cursor.description]

        def create_row(*args):
            return dict(zip(columnNames, args))

        return create_row

    def query_dict(self, sql):
        """查询sql,返回list数据集 dict"""
        query_dict_res = ''
        try:
            # 执行sql语句
            self._cursor_dict.execute(sql)
            self._cursor_dict.rowfactory = self.make_dict_factory(self._cursor_dict)
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
            self._conn.rollback()
        return execute_res


if __name__ == '__main__':
    oracle = Oracle(host='10.0.23.109', user='system', password='oracle123', db='haswj1dw')
    res = oracle.query_dict('select * from HX_SB.WAIT_DELETE')
    res2 = oracle.query_tuple('select * from HX_SB.WAIT_DELETE')
    oracle.execute("update HX_SB.WAIT_DELETE set  HOSTNAME='1' where TASKNAME='2'")
    oracle.close()
