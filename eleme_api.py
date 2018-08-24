# -*- coding: utf-8 -*-
"""
    Created by Catelemmon on 2018/7/17 0017
"""
import codecs
import copy
import json
import random
from typing import Tuple
import redis
import requests
from Geohash import geohash
from pypinyin import lazy_pinyin
from sqlalchemy import Column, String, create_engine, BigInteger, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

__author__ = "Catelemmon"

REDIS_SERVER_IP = "127.0.0.1"
# redis 的服务器端口
REDIS_SERVER_PORT = 6379
# 地图的抓取队列
AMAP_STREET_QUEUE = "street_queue"

RESTA_LIST_URL_TEMPLATE = \
    "https://www.ele.me/restapi/shopping/restaurants?extras%5B%5D=activities&geohash={geohash}&latitude={lat}&" + \
    "limit=24&longitude={lon}&offset={page_num}&terminal=web"
RESTA_DETAIL_URL_TEMPLATE = \
    "https://www.ele.me/restapi/ugc/v1/restaurants/{shop_id}/rating_scores?latitude={lat}&longitude={lon}"
HEADERS_TEMPLATE = {
    "authority": "www.ele.me",
    "method": "GET",
    #  30.538512  104.083384 经纬度的位数
    "path": "/restapi/shopping/restaurants?extras%5B%5D" +
            "=activities&geohash={geohash}&latitude={lat}&limit=24&longitude={lon}&offset={page_num}&terminal=web",
    "scheme": "https",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
                  " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9",
    "x-shared": "loc={loc_lat},{loc_lon}"
}
RESTAURANT_DETAIL_HEADER = {
    "authority": "www.ele.me",
    "method": "GET",
    "path": "/restapi/ugc/v1/restaurants/{shop_id}/rating_scores?latitude={lat}&longitude={lon}",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9",
    "referer": "https://www.ele.me/shop/{shop_id}/rate",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/66.0.3359.139 Safari/537.36",
    "x-shard": "shopid={shop_id};loc={loc}",
}

STATIC_INDEX_HEADER = {
    "authority": "www.ele.me",
    "method": "GET",
    "path": "/restapi/shopping/restaurants?extras%5B%5D=activities&geohash=wm6jbe7x9b3r&latitude=30.561135&limit=24&"
             "longitude=104.089356&offset=48&terminal=web",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9",
    "cookie": "ubt_ssid=7l08wgasuhmmgdaw2byqgrsh84w44ros_2018-07-18; _"
              "utrace=497642646535a0dbb456ac984937d93d_2018-07-18"
              "; eleme__ele_me=124bcc4b626d5757120c3f21684bc6ad%3A7cc5a72da703c14339bb5f905500b4f624186ab5; "
              "track_id=1531878566|13186c9ba444ce8fb869a36a4d3d7635f57acd214f53d4c455|9786865f872c4a4d3b9a6f01237e50fd; "
              "USERID=171646298; SID=q6SuhLmTUxJjMza1YWyVN4Fu31cK8143Q0dQ",
    "referer": "https://www.ele.me/place/wm6jbe7x9b3r?latitude=30.561135&longitude=104.089356",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/66.0.3359.139 Safari/537.36",
    "x-shard": "loc=104.089356,30.561135",
}
STATIC_DETAIL_HEADER = {
    "authority": "www.ele.me",
    "method": "GET",
    "path": "/restapi/shopping/restaurants?extras%5B%5D=activities&geohash=wm6jbe7x9b3r&latitude=30.561135&limit=24&longitude=104.089356&offset=48&terminal=web",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9",
    "cookie": "ubt_ssid=7l08wgasuhmmgdaw2byqgrsh84w44ros_2018-07-18; _utrace=497642646535a0dbb456ac984937d93d_2018-07-18; eleme__ele_me=124bcc4b626d5757120c3f21684bc6ad%3A7cc5a72da703c14339bb5f905500b4f624186ab5; track_id=1531878566|13186c9ba444ce8fb869a36a4d3d7635f57acd214f53d4c455|9786865f872c4a4d3b9a6f01237e50fd; USERID=171646298; SID=q6SuhLmTUxJjMza1YWyVN4Fu31cK8143Q0dQ",
    "referer": "https://www.ele.me/place/wm6jbe7x9b3r?latitude=30.561135&longitude=104.089356",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
    "x-shard": "loc=104.089356,30.561135",
}


redis_pool = redis.ConnectionPool(host=REDIS_SERVER_IP, port=REDIS_SERVER_PORT, db=0)


def fake_local(nearby_lat, nearby_lon):
    nearby_lat = random.uniform(nearby_lat - 0.00004, nearby_lat + 0.00004)
    nearby_lon = random.uniform(nearby_lon - 0.00004, nearby_lon + 0.00004)
    return float('%.6f' % nearby_lat), float('%.6f' % nearby_lon)


def get_cities():
    response = requests.get("https://www.ele.me/restapi/shopping/v1/cities")
    json_object_cities = json.loads(response.text)
    json_str_cites = json.dumps(json_object_cities, indent=" ", ensure_ascii=False)
    with codecs.open("eleme_cities.ini", "w", encoding="utf-8") as f_cites:
        f_cites.write(json_str_cites)
    return json_str_cites


def get_city_location(city_name, f_city) -> Tuple[float, float]:
    with codecs.open(f_city, "r", encoding="utf-8") as f_cities:
        cities = json.load(f_cities)
    city_initials = get_initials(city_name)
    for city_item in cities[city_initials[0]]:
        if city_item["name"] == city_name:
            return city_item["latitude"], city_item["longitude"]
    raise StopIteration(
        'Unknow {city_name} in cities list ! Please enter thr correct city name!'.format(city_name=city_name))


def get_initials(name):
    pinyin_list = lazy_pinyin(name)
    s_initial = ""
    for i in pinyin_list:
        s_initial += str.upper(i[0])
    return s_initial


def get_geohash(latitude, longitude):
    return geohash.encode(latitude=latitude, longitude=longitude)


def get_street_lonlat():
    redis_client = redis.Redis(connection_pool=redis_pool)
    while redis_client.llen(AMAP_STREET_QUEUE) > 0:
        r_lon, r_lat = str(redis_client.brpop(AMAP_STREET_QUEUE)[1], encoding="utf-8").split(',')
        yield r_lon, r_lat
    raise StopIteration("the location has been ran out! ")


def build_header(aim_geohash, aim_lat, aim_lon, page_num, fake_loc_lat, fake_loc_lon):
    cookies = ""
    with codecs.open("eleme_cookies.ini", "r") as fcookies:
        jcookies = json.load(fcookies)
    for i in jcookies:
        cookies += (i["name"] + "+" + i["value"] + " ")
    headers = copy.deepcopy(HEADERS_TEMPLATE)
    headers["cookie"] = cookies
    headers["path"] = headers["path"].format(geohash=aim_geohash, lat=aim_lat, lon=aim_lon, page_num=page_num)
    headers["x-shared"] = headers["x-shared"].format(loc_lat=fake_loc_lat, loc_lon=fake_loc_lon)
    return headers


def build_request_url(aim_geohash, aim_lat, aim_lon, page_num):
    url = RESTA_LIST_URL_TEMPLATE.format(geohash=aim_geohash, lat=aim_lat, lon=aim_lon, page_num=page_num)
    return url


def eleme_api_request(api_url, api_header):
    response = requests.get(url=api_url, headers=STATIC_INDEX_HEADER)
    parse_resta_items(response.text)
    return response.text


def has_resta(resta_id):
    redis_client = redis.Redis(connection_pool=redis_pool)
    if redis_client.sismember("restaurant_dupfilter", resta_id):
        return True
    else:
        redis_client.sadd("restaurant_dupfilter", resta_id)
        return False


def parse_resta_items(resta_sjson):
    """
    :param resta_sjson: str
    :return: List[dict]
    """
    resta_json = json.loads(resta_sjson)
    for resta_iter in resta_json:
        resta_id = resta_iter["id"]
        if not has_resta(resta_id):
            resta_item = {"name": resta_iter["name"], "id": resta_iter["id"], "phone": resta_iter["phone"],
                          "rating": resta_iter["rating"], "address": resta_iter["address"], "longitude":
                              resta_iter["longitude"], "latitude": resta_iter["latitude"]}
            details = request_resta_details(resta_item["id"], resta_item["latitude"], resta_item["longitude"])
            resta_item = dict(resta_item.items(), **details)
            save_to_mysql(resta_item)
        else:
            continue


def request_resta_details(restaurant_id, restaurant_lat, restaurant_lon):
    def build_detail_url(shop_id, shop_lat, shop_lon):
        return RESTA_DETAIL_URL_TEMPLATE.format(shop_id=shop_id, lat=shop_lat, lon=shop_lon)

    def build_detail_header(shop_id, shop_lat, shop_lon):
        cookies = ""
        header = copy.deepcopy(RESTAURANT_DETAIL_HEADER)
        header["path"] = header["path"].format(shop_id=shop_id, lat=shop_lat, lon=shop_lon)
        header["referer"] = header["referer"].format(shop_id=shop_id)
        header["x-shard"] = header["x-shard"].format(shop_id=shop_id, loc=str(shop_lon) + "," + str(shop_lat))
        with codecs.open("eleme_cookies.ini", "r") as fcookies:
            jcookies = json.load(fcookies)
        for i in jcookies:
            cookies += (i["name"] + "+" + i["value"] + " ")
        header["cookie"] = cookies
        return header

    def parse_detail(score_json):
        scores = json.loads(score_json)
        return {'compare_rating': str(round(scores.get("compare_rating", 0), 4) * 100)[:4] + "%", 'food_score':
            round(scores.get("food_score", 0), 2),
                "positive_rating": str(scores.get("positive_rating", 0) * 100) + "%"}

    url = build_detail_url(restaurant_id, restaurant_lat, restaurant_lon)
    headers = build_detail_header(restaurant_id, restaurant_lat, restaurant_lon)
    response = requests.get(url=url, headers=STATIC_DETAIL_HEADER)
    return parse_detail(response.text)


CHINESE_TO_ENGLISH = {
    "id": "店铺id",
    "name": "店铺名称",
    "phone": "手机号码",
    "rating": "评分",
    "address": "店铺地址地址",
    "compare_rating": "高于周边商家",
    "food_score": "食物评分",
    "positive_rating": "好评率"
}

# 创建对象的基类:
Base = declarative_base()


class Restaurant(Base):
    __tablename__ = "restaurant"

    restaurant_id = Column(BigInteger, primary_key=True)
    restaurant_name = Column(String(50))
    phone = Column(String(100))
    rating = Column(String(8))
    address = Column(String(200))
    compare_rating = Column(String(8))
    food_score = Column(String(8))
    positive_rating = Column(String(8))


engine = create_engine("mysql+pymysql://root:09170725@localhost:3306/restaurant")
DBSession = sessionmaker(bind=engine)
metadata = MetaData(engine)
Base.metadata.create_all(engine)
session = DBSession()


def save_to_mysql(resta_item):
    """
    :param resta_item: dict
    :return:
    """
    new_resta = Restaurant(restaurant_name=resta_item["name"], phone=resta_item["phone"],
                           rating=str(resta_item.get("rating", 0)), address=resta_item["address"],
                           compare_rating=str(resta_item.get("compare_rating", 0)), food_score=str(resta_item.get("food_score", 0)),
                           positive_rating=resta_item.get("positive_rating", 0), restaurant_id=resta_item["id"]
                           )
    session.add(new_resta)
    session.commit()


def start_spider(page_count):
    location_gen = get_street_lonlat()
    while True:
        try:
            lon, lat = next(location_gen)
            street_geohash = get_geohash(float(lat), float(lon))
            fake_lon, fake_lat = fake_local(float(lon), float(lat))
            header_dic = build_header(street_geohash, lat, lon, 0, fake_lat, fake_lon)
            for i in range(0, page_count):
                restaurant_api_url = build_request_url(street_geohash, lat, lon, page_num=i*24)
                eleme_api_request(restaurant_api_url, header_dic)
        except StopIteration:
            break


if __name__ == '__main__':
    start_spider(25)
