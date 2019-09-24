#!/usr/bin/python3.7
# -*- coding: utf8 -*-

import requests
import json
from bs4 import BeautifulSoup

import os
import psycopg2

import play_scraper

import datetime

import sys

args = sys.argv

DATABASE_URL='postgresql://'+ args[1] + ':' + args[2] + '@'+ args[3] + ':5439/'+ args[4]
LOG='/tmp/superset.log'
DEBUG=True

def get_connection():
    return psycopg2.connect(DATABASE_URL)

# 昔ながらのスクレイピング
#target_url = 'https://play.google.com/store/apps/collection/topselling_new_free?hl=ja'
#target_url = 'http://play.google.com/store/apps/category/GAME/collection/topselling_new_free?authuser=0'
#target_url = 'https://play.google.com/store/apps/category/GAME_RACING/collection/topselling_new_free'
target_url = 'https://play.google.com/store/apps/collection/cluster?clp=CiAKHgoYdG9wc2VsbGluZ19uZXdfZnJlZV9HQU1FEAcYAw%3D%3D:S:ANO1ljJaN44'
r = requests.get(target_url)         #requestsを使って、webから取得
soup = BeautifulSoup(r.text, 'lxml') #要素を抽出

with get_connection() as conn:
    with conn.cursor() as cur:
        i=1
        json_detail={}
        for j in soup.find_all('script'):
            if j.string is not None:
                if -1 != j.string.find("""AF_initDataCallback({key: 'ds:3"""):
                    json_detail = json.loads(j.string[86:-4])
                    for app_id in json_detail[0][1][0][0][0]:
                        cur.execute("INSERT INTO superset_schema.app_ids(app_id, platform, created_at) SELECT '" + app_id[12][0] + "', 1, CONVERT_TIMEZONE('JST', SYSDATE) WHERE NOT EXISTS ( SELECT app_id FROM superset_schema.app_ids WHERE app_id = '" + app_id[12][0] + "'); ")
                        if DEBUG:
                            print(str(i) + ": " + app_id[12][0])
                        i=i+1


# 簡単にスクレイピング
# Todo コネクション多すぎるのでforをコネクションの中に入れる
count=1
for COLLECTION_NAME in ['NEW_FREE', 'TOP_FREE']:
    for i in range(5):
        if DEBUG:
            print("{}".format(COLLECTION_NAME))
        collections=play_scraper.collection(collection=COLLECTION_NAME, results=120, page=i)
        for collection in collections:
            detail=play_scraper.details(collection['app_id'])
            if -1 != detail['category'][0].find("GAME"):
    
                with get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("INSERT INTO superset_schema.app_ids(app_id, platform, created_at) SELECT %s, 1, CONVERT_TIMEZONE('JST', SYSDATE) WHERE NOT EXISTS ( SELECT app_id FROM superset_schema.app_ids WHERE app_id = %s); ", (detail['app_id'], detail['app_id']))
                        if DEBUG:
                            print(str(count) + ": " + detail['app_id'])
                        count=count+1

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": get_new_arrival_aos\n")

