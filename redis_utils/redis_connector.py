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
# 方式1
import redis

r1 = redis.StrictRedis(host='localhost', port=6379, db=0)
r1.set('foo', 'bar')  # True

r1.get('foo')  # 'bar'

# 方式2
r2 = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
r2.set('name', 'n')  # 设置 name 对应的值

print(r2['name'])
print(r2.get('name'))  # 取出键 name 对应的值
print(type(r2.get('name')))  # 查看类型
