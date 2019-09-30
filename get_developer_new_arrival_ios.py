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
DEBUG=False

def get_connection():
    return psycopg2.connect(DATABASE_URL)

# redshiftからapp_idリストを取得 joinで不足分だけに変更する
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT publisher_id, publisher_name FROM superset_schema.app_details WHERE platform = 2 ORDER BY publisher_id ;")
        for dev in cur:
            dev_id=dev[1].replace(" ", "-").replace(".","").replace(",","").replace(":","").replace(";",'').lower()

            # iTunesからページ取得
            target_url = "https://itunes.apple.com/search?entity=software&country=jp&lang=ja_jp&limit=200&term={dev_id}".format(dev_id=dev_id)
            response = requests.get(target_url, timeout=(10.0, 10.0)) #requestsを使って、webから取得
            # リトライ
            if response.status_code != requests.codes.ok:
                response = requests.get(target_url, timeout=(10.0, 10.0)) #requestsを使って、webから取得
            app_dict = json.loads(response.text)

            # 個数チェック
            if app_dict['resultCount'] == 0:
                target_url = "https://itunes.apple.com/search?entity=software&country=us&lang=ja_jp&limit=200&term={dev_id}".format(dev_id=dev_id)
                response = requests.get(target_url, timeout=(10.0, 10.0)) #requestsを使って、webから取得
                # リトライ
                if response.status_code != requests.codes.ok:
                    response = requests.get(target_url, timeout=(10.0, 10.0)) #requestsを使って、webから取得

                app_dict = json.loads(response.text)

            # 個数チェック
            if app_dict['resultCount'] == 0:
                target_url = "https://itunes.apple.com/search?entity=software&country=cn&lang=ja_jp&limit=200&term={dev_id}".format(dev_id=dev_id)
                response = requests.get(target_url, timeout=(10.0, 10.0)) #requestsを使って、webから取得
                # リトライ
                if response.status_code != requests.codes.ok:
                    response = requests.get(target_url, timeout=(10.0, 10.0)) #requestsを使って、webから取得
                app_dict = json.loads(response.text)
 
            for app in app_dict['results']:
                if app['primaryGenreId'] == 6014:
                    if DEBUG:
                        print(app['trackName'])
                        print(app['sellerName'])
                        print(app['trackViewUrl'])

                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("INSERT INTO superset_schema.app_ids(app_id, platform, created_at) SELECT '" + str(app['trackId']) + "', 2, CONVERT_TIMEZONE('JST', SYSDATE) WHERE NOT EXISTS ( SELECT app_id FROM superset_schema.app_ids WHERE app_id = '" + str(app['trackId']) + "'); ")

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": get_developer_new_arrival_ios\n")

