# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @time ：2021/5/27
# @desc : description

from pymongo import MongoClient


class Mongo(object):
    def __init__(self, host='localhost', port=27017):
        self.host = host
        self.port = int(port)
        self.client = MongoClient('mongodb://{}:{}/'.format(self.host, self.port))
