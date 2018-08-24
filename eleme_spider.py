# -*- coding: utf-8 -*-
"""
    Created by Catelemmon on 2018/7/14 0014
"""

import random
from Geohash import geohash

__author__ = "Catelemmon"

# lat 102°54′～104°53 lon 30°05 ′～31°26′
CHENGDU_LAT_LON = {"lat_min": 102.54, "lat_max": 104.53,
                   "lon_min": 30.05, "lon_max": 31.26}


def build_geohash():
    for lat in range(int(CHENGDU_LAT_LON["lat_min"]*1000000), int(CHENGDU_LAT_LON["lat_max"]*1000000), 50000):
        for lon in range(int(CHENGDU_LAT_LON["lon_min"]*1000000), int(CHENGDU_LAT_LON["lon_max"]*1000000), 50000):
            yield geohash.encode(lat/1000000, lon/1000000), lat/1000000, lon/1000000


def fake_local(lat, lon):
    lat = random.uniform(lat - 0.00002, lat + 0.00002)
    lon = random.uniform(lon - 0.00002, lon + 0.00002)
    return {"loc_lat": lat, "loc_lon": lon}


if __name__ == '__main__':
    for i in build_geohash():
        print(i)
