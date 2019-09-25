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

# スクレイピング
#target_jp_url = 'http://ax.itunes.apple.com/WebObjects/MZStoreServices.woa/ws/RSS/topfreeapplications/sf=143441/limit=100/genre=6014/lang=ja_jp/json'
target_jp_url = 'https://itunes.apple.com/jp/rss/topfreeapplications/limit=100/genre=6014/lang=ja_jp/json'
target_us_url = 'https://itunes.apple.com/us/rss/topfreeapplications/limit=100/genre=6014/lang=ja_jp/json'
target_cn_url = 'https://itunes.apple.com/cn/rss/topfreeapplications/limit=100/genre=6014/lang=ja_jp/json'
json_jp = json.loads(requests.get(target_jp_url).text) #requestsを使って、webから取得
json_us = json.loads(requests.get(target_us_url).text) #requestsを使って、webから取得
json_cn = json.loads(requests.get(target_cn_url).text) #requestsを使って、webから取得
json=[]
for entry_jp in json_jp['feed']['entry']:
    json.append(entry_jp)

for entry_us in json_us['feed']['entry']:
    json.append(entry_us)

for entry_cn in json_cn['feed']['entry']:
    json.append(entry_cn)

#for entry in json:
#    print(entry['im:name']['label'])
#    print(entry['im:name'])

with get_connection() as conn:
    with conn.cursor() as cur:
#        json_jp = json.loads(requests.get(target_jp_url).text) #requestsを使って、webから取得
        i=1
#        for entry in json_jp['feed']['entry']:
        for entry in json:
            if DEBUG:
                print(str(i) + ": " + entry['im:name']['label'] + " : " + entry['id']['attributes']['im:id'])
            i=i+1
            cur.execute("INSERT INTO superset_schema.app_ids(app_id, platform, created_at) SELECT '" + entry['id']['attributes']['im:id'] + "', 2, CONVERT_TIMEZONE('JST', SYSDATE) WHERE NOT EXISTS ( SELECT app_id FROM superset_schema.app_ids WHERE app_id = '" + entry['id']['attributes']['im:id'] + "'); ")

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": get_new_arrival_ios\n")

