# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @time ：2021/5/26
# @desc : description
import vertica_python


class Vertica(object):
    def __init__(self, host='127.0.0.1', port=5433, user='root', password='', database='test'):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        self._conn_dict = self.__connect()
        self._conn = self.__connect()  # vertica 单个conn在创建cursor的时候会互相影响
        if self._conn_dict and self._conn:
            self._cursor_tuple = self._conn.cursor()
            self._cursor_dict = self._conn_dict.cursor('dict')

    def __connect(self):
        try:
            conn = vertica_python.connect(host=self.host, port=self.port, user=self.user,
                                          password=self.password, database=self.database)
        except Exception as e:
            print("连接异常:", e)
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
                if type(self._conn_dict == 'object'):
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

    def query_dict(self, sql):
        """查询sql,返回list数据集 dict"""
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
            self._conn.rollback()
        return execute_res


if __name__ == '__main__':
    ver = Vertica(host='', port=5433, user='', password='', database='')
    res = ver.query_dict('select * from sq_xtyw.sjbd_config')
    res2 = ver.query_tuple('select * from sq_xtyw.sjbd_config')
    res3 = ver.execute("insert into sq_xtyw.sjbd_config values (1,'2','3','2','1223');")
    ver.close()
