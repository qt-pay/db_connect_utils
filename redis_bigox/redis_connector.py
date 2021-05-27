# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @time ：2021/5/27
# @desc : description
"""
redis 提供两个类 Redis 和 StrictRedis,
    StrictRedis 用于实现大部分官方的命令，
    Redis 是 StrictRedis 的子类，用于向后兼用旧版本。
redis 取出的结果默认是字节，我们可以设定 decode_responses=True 改成字符串。
"""
import redis


# 方式1 用于实现大部分官方的命令，
class Redis(object):
    def __init__(self, host='localhost', port=6379, db=0, decode_responses=True):
        self.host = host
        self.port = int(port)
        self.db = db
        self.decode_responses = decode_responses
        self.conn = redis.StrictRedis(host=self.host, port=self.port, db=self.db,
                                      decode_responses=self.decode_responses)

    def get_data_demo(self):
        """查询示例"""
        print(self.conn.get('name').decode('utf-8'))


# 方式2  是 StrictRedis 的子类，用于向后兼用旧版本。
class Redis(object):
    def __init__(self, host='localhost', port=6379, db=0, decode_responses=True):
        self.host = host
        self.port = int(port)
        self.db = db
        self.decode_responses = decode_responses
        self.conn = redis.Redis(host=self.host, port=self.port, db=self.db,
                                decode_responses=self.decode_responses)

    def get_data_demo(self):
        """查询示例"""
        print(self.conn.get('name').decode('utf-8'))
