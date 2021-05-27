# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @time ：2021/5/26
# @desc : 数据库连接池

from threading import Lock
import vertica_python
from DBUtils.PersistentDB import PersistentDB  # 线程安全
from DBUtils.PooledDB import PooledDB  # 线程不安全; python3 请使用: pip install DBUtils==1.3


class VerticaPool(object):
    __pool = None  # 类属性, 所有实例公用变量
    __lock = Lock()

    def __init__(self, mincached=10, maxcached=20, maxshared=10, maxconnections=200, blocking=True,
                 maxusage=100, setsession=None, reset=True,
                 host='127.0.0.1', port=5433, user='root', password='', database='test', charset='utf8mb4'):
        """
        dbapi ：数据库接口
        :param mincached:连接池中空闲连接的初始数量
        :param maxcached:连接池中空闲连接的最大数量
        :param maxshared:共享连接的最大数量
        :param maxconnections:创建连接池的最大数量
        :param blocking:超过最大连接数量时候的表现，为True等待连接数量下降，为false直接报错处理
        :param maxusage:单个连接的最大重复使用次数
        :param setsession:optional list of SQL commands that may serve to prepare
            the session, e.g. ["set datestyle to ...", "set time zone ..."]
        :param reset:how connections should be reset when returned to the pool
            (False or None to rollback transcations started with begin(),
            True to always issue a rollback for safety's sake)
        :param host:数据库ip地址
        :param port:数据库端口
        :param db:库名
        :param user:用户名
        :param passwd:密码
        :param charset:字符编码
        """

        if not self.__pool:
            with self.__lock:
                self.__class__.__pool = PooledDB(vertica_python, mincached, maxcached,
                                                 maxshared, maxconnections, blocking,
                                                 maxusage, setsession, reset,
                                                 host=host, port=port, database=database,
                                                 user=user, password=password)
        self._conn = self.__pool.connection()
        self._conn_dict = self.__pool.connection()
        if self._conn:
            self._cursor_tuple = self._conn.cursor()
            self._cursor_dict = self._conn_dict.cursor('dict')

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
    ver = VerticaPool(host='', port=5433, user='', password='', database='')
    res = ver.query_dict('select * from sq_xtyw.sjbd_config')
    res2 = ver.query_tuple('select * from sq_xtyw.sjbd_config')
    res3 = ver.execute("insert into sq_xtyw.sjbd_config values (1,'2','3','2','1223');")
    ver.close()
