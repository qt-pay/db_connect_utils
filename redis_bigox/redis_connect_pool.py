# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @time ：2021/5/27
# @desc : redis 连接池
"""
redis 提供两个类 Redis 和 StrictRedis,
    StrictRedis 用于实现大部分官方的命令，
    Redis 是 StrictRedis 的子类，用于向后兼用旧版本。
redis 取出的结果默认是字节，我们可以设定 decode_responses=True 改成字符串。

并发操作操作，就需要写成线程安全的单例,最简单的实现方式是
写在一个文件里面，然后再另一个文件中导入,就是一个单例模式
"""
import redis
from threading import Lock


class RedisPool(object):
    __redis_pool = None
    __lock = Lock()

    def __init__(self, host='127.0.0.1', port=6379, db=0):
        self.host = host
        self.port = int(port)
        self.db = db
        self.redis_pool = self.__connect()
        if self.redis_pool:
            # redis 取出的结果默认是字节，设定 decode_responses=True 改成字符串
            self.conn = redis.Redis(connection_pool=self.redis_pool, decode_responses=True)

    def __connect(self):
        with self.__lock:
            if not self.__redis_pool:
                self.__redis_pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db)
        return self.__redis_pool

    def get_data_demo(self):
        """查询示例"""
        print(self.conn.get('name').decode('utf-8'))
