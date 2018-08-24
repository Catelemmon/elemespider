# -*- coding: utf-8 -*-
"""
    Created by Catelemmon on 2018/7/17 0017
"""
import json
from queue import Queue
from typing import Dict

import redis
import requests

__author__ = "Catelemmon"


# 30.5702,104.064758 默认的初始地址
INITIAL_LOCACTION = "30.5702,104.064758"  # 成都
# redis数据库的ip地址
REDIS_SERVER_IP = "127.0.0.1"
# redis 的服务器端口
REDIS_SERVER_PORT = 6379
# 地图的set
AMAP_STREET_SET = "street_set"
# 地图的抓取队列
AMAP_STREET_QUEUE = "street_queue"
# 高德地图最大请求次数
AMAP_REQUEST_LIMIT = 5000
# 高德地图的api
AMAP_API = "https://restapi.amap.com/v3/place/around?" \
           "key=d7df718d2ff4e1e8dd764f59b0663d22" \
           "&location={location}&keywords=&types=190301" \
           "&radius=3000&offset={offset}&page={page_num}&extensions=all"


class StreetQueue(object):
    def __init__(self, initial_location=INITIAL_LOCACTION,  host=REDIS_SERVER_IP, port=REDIS_SERVER_PORT, db=0):
        self.__pool = redis.ConnectionPool(host=host, port=port, db=db)
        self.__client = redis.Redis(connection_pool=self.__pool)
        self.__set = AMAP_STREET_SET
        self.street_queue = AMAP_STREET_QUEUE
        self.initial_location = initial_location
        self.amap_request_limit = AMAP_REQUEST_LIMIT
        self.location_queue = Queue()
        self.location_queue.put(INITIAL_LOCACTION)

    def exist_street(self, street_id):
        if self.__client.sismember(self.__set, street_id):
            return True
        else:
            return False

    def add_street(self, street_id, street_location):
        if not self.exist_street(street_id):
            self.__client.sadd(self.__set, street_id)
            self.__client.lpush(self.street_queue, street_location)
        else:
            pass

    def request_amap_api(self, page_num, initial_location):
        if self.amap_request_limit > 0:
            url = AMAP_API.format(location=initial_location, offset=50, page_num=page_num)
            self.amap_request_limit -= 1
            response = requests.get(url)
            return response
        else:
            raise Exception("the free request-time has used up !")

    def start_request_street(self,):
        while not self.location_queue.empty():
            page_num = 1
            location = self.location_queue.get()
            response = self.request_amap_api(page_num, location)
            street_data = json.loads(response.text)
            if not street_data["infocode"] == "10000":
                raise Exception("the free request-time has used up !")
            total = int(street_data["count"])
            if total == 0:
                pass
            current_count = len(street_data["pois"])
            while current_count < total:
                self.parse_street(street_data["pois"])
                page_num += 1
                response = self.request_amap_api(page_num, location)
                street_data = json.loads(response.text)
                current_count += len(street_data.get("pois", []))

    def parse_street(self, pois):
        for street_item in pois:
            street_item: Dict[str, str]
            self.add_street(street_item["id"], street_item["location"])
            lon, lat = street_item["location"].split(",")
            # self.start_request_street(lat+","+lon)
            self.location_queue.put(lat+","+lon)


if __name__ == '__main__':
    sq = StreetQueue()
    sq.start_request_street()