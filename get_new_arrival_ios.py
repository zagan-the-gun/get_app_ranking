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

# JSON取得

# 無料トップゲーム
#target_jp_url = 'http://ax.itunes.apple.com/WebObjects/MZStoreServices.woa/ws/RSS/topfreeapplications/sf=143441/limit=100/genre=6014/lang=ja_jp/json'
target_jp_url = 'https://itunes.apple.com/jp/rss/topfreeapplications/limit=100/genre=6014/lang=ja_jp/json'
target_us_url = 'https://itunes.apple.com/us/rss/topfreeapplications/limit=100/genre=6014/lang=ja_jp/json'
target_cn_url = 'https://itunes.apple.com/cn/rss/topfreeapplications/limit=100/genre=6014/lang=ja_jp/json'

#requestsを使って、webから取得
json_jp = json.loads(requests.get(target_jp_url).text)
json_us = json.loads(requests.get(target_us_url).text)
json_cn = json.loads(requests.get(target_cn_url).text)

app_dict=[]
for entry_jp in json_jp['feed']['entry']:
    app_dict.append(entry_jp)

for entry_us in json_us['feed']['entry']:
    app_dict.append(entry_us)

for entry_cn in json_cn['feed']['entry']:
    app_dict.append(entry_cn)

# 無料おすすめ新着ゲームJSON
target_new_jp_url = 'https://rss.itunes.apple.com/api/v1/jp/ios-apps/new-games-we-love/all/100/explicit.json'
target_new_us_url = 'https://rss.itunes.apple.com/api/v1/us/ios-apps/new-games-we-love/all/100/explicit.json'
target_new_cn_url = 'https://rss.itunes.apple.com/api/v1/cn/ios-apps/new-games-we-love/all/100/explicit.json'

#requestsを使って、webから取得
dict_new_jp = json.loads(requests.get(target_new_jp_url).text)
dict_new_us = json.loads(requests.get(target_new_us_url).text)
dict_new_cn = json.loads(requests.get(target_new_cn_url).text)

new_app_dict=[]
for results_jp in dict_new_jp['feed']['results']:
    new_app_dict.append(results_jp)
for results_us in dict_new_us['feed']['results']:
    new_app_dict.append(results_us)
for results_cn in dict_new_cn['feed']['results']:
    new_app_dict.append(results_cn)

#print(app_dict)

with get_connection() as conn:
    with conn.cursor() as cur:
        i=1
        # 無料トップゲーム
        for entry in app_dict:
            if DEBUG:
                print(str(i) + ": " + entry['im:name']['label'] + " : " + entry['id']['attributes']['im:id'])
            i=i+1
            cur.execute("INSERT INTO superset_schema.app_ids(app_id, platform, created_at) SELECT '" + entry['id']['attributes']['im:id'] + "', 2, CONVERT_TIMEZONE('JST', SYSDATE) WHERE NOT EXISTS ( SELECT app_id FROM superset_schema.app_ids WHERE app_id = '" + entry['id']['attributes']['im:id'] + "'); ")

        # おすすめ新着ゲーム
        for results in new_app_dict:
            if DEBUG:
                print(str(i) + ": " + results['name'] + " : " + results['id'])
            i=i+1
            cur.execute("INSERT INTO superset_schema.app_ids(app_id, platform, created_at) SELECT '" + entry['id']['attributes']['im:id'] + "', 2, CONVERT_TIMEZONE('JST', SYSDATE) WHERE NOT EXISTS ( SELECT app_id FROM superset_schema.app_ids WHERE app_id = '" + entry['id']['attributes']['im:id'] + "'); ")

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": get_new_arrival_ios\n")

