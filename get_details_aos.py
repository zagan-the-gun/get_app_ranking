#!/bin/python3.7
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
        cur.execute("SELECT app_id FROM superset_schema.app_ids EXCEPT SELECT app_id FROM superset_schema.app_details;")
        for app_id in cur:

            # google playからページ取得
            target_url = "https://play.google.com/store/apps/details?id={app_id}".format(app_id=app_id[0])
            response = requests.get(target_url) #requestsを使って、webから取得

            # スクレイピング１
            r = requests.get(target_url)
            soup = BeautifulSoup(r.text, 'lxml') #要素を抽出
            if soup.find('script', type="application/ld+json") is None:
                if DEBUG:
                    print("app is dead")
                continue
            json_response = json.loads(soup.find('script', type="application/ld+json").string)

            # スクレイピング2 (海外ゲームで動かない)
            json_detail={}
            for j in soup.find_all('script'):
                if j.string is not None:
                    if -1 != j.string.find("""AF_initDataCallback({key: 'ds:5"""):
                        json_detail = json.loads(j.string[86:-4])


            # スクレイピング3
            detail = play_scraper.details(app_id[0])

            #if 'aggregateRating' in json_response:
            # レーティングチェック
            # 誰にも評価されてないと空っぽ?
            if detail['score'] is not None:
                RATING = detail['score']
                RATING_COUNT = str(json_response['aggregateRating']['ratingCount'])
            else:
                RATING = 0
                RATING_COUNT = 0

            # インストール数チェック
            if json_detail[0][12][9][2] is not None:
                INSTALLS = json_detail[0][12][9][2]
            elif detail['installs'] is not None:
                INSTALLS = detail['installs'].replace("+", "").replace(",", "")
            else:
                INSTALLS = 0

            if DEBUG:
                print("----------------------")
                print("app_id            : {}".format(app_id[0]))
                print("app_name          : {}".format(json_response['name']))
                print("platform          : 1")
                print("icon_url          : {}".format(json_response['image']))
                print("ranking           : -")
                print("ranking_update_at : -now-")
                print("rating            : {}".format(RATING))
                print("rating_count      : {}".format(RATING_COUNT))
                print("rating_update_at  : -now-")
                print("genere            : {}".format(json_response['applicationCategory']))
                #print("installs:          {}".format(json_detail[0][12][9][2]))
                print("installs          : {}".format(INSTALLS))
                print("price             : {}".format(json_response['offers'][0]['price']))
                #print("publisher_id:      {}".format(json_detail[0][12][5][0][0]))
                print("publisher_id      : {}".format(detail['developer_id']))
                #print("publisher_name:    {}".format(json_detail[0][12][5][1]))
                print("publisher_name    : {}".format(detail['developer']))

            # redshiftに詳細データを書き込む
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO superset_schema.app_details (app_id, app_name, platform, icon_url, ranking, ranking_update_at, rating, rating_count, rating_update_at, genre, installs, price, publisher_id, publisher_name, created_at) SELECT %s, %s, 1, %s, 0, convert_timezone('jst', sysdate), %s, %s, convert_timezone('jst', sysdate), %s, %s, %s, %s, %s, convert_timezone('jst', sysdate) where not exists ( select app_id from superset_schema.app_details where app_id = %s); ",
                            (app_id[0], json_response['name'], json_response['image'], RATING, RATING_COUNT, json_response['applicationCategory'], INSTALLS, json_response['offers'][0]['price'], detail['developer_id'], detail['developer'], app_id[0]))

with open(LOG, mode='a') as f:
    f.write("get_details_aos: "+str(datetime.datetime.now())+"\n")

