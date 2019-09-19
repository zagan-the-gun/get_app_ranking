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
            # ランゲージ選択するとインストール数とか諸々数字が取れなくなるバグがあるのでしょうがない
            detail_ja = play_scraper.details(app_id[0], hl='ja')

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

            # ヒストグラムチェック
            HISTOGRAM={}
            if detail['histogram'] != {}:
                for h in detail['histogram'].items():
                    if h[1] is None:
                        HISTOGRAM[h[0]]=0
                    else:
                        HISTOGRAM[h[0]]=h[1]
            else:
                for i in range(1, 6):
                    HISTOGRAM[i]=0

            # スクリーンショットjson化
            if detail['screenshots'] is not None:
                SCREENSHOTS = str(detail['screenshots']).replace("\'", "\"")
            else:
                SCREENSHOTS = ""

            # コンテンツレーティングjson化
            if detail['content_rating'] is not None:
                CONTENT_RATING = str(detail['content_rating']).replace("\'", "\"")
            else:
                CONTENT_RATING = ""

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
                print("installs          : {}".format(INSTALLS))
                print("price             : {}".format(json_response['offers'][0]['price']))
                print("publisher_id      : {}".format(detail['developer_id']))
                print("publisher_name    : {}".format(detail['developer']))
#                print("release_at        : {}".format("NULL"))
                print("description       : {}".format(detail_ja['description']))
                print("screenshots       : {}".format(SCREENSHOTS))
                print("video             : {}".format(detail['video']))
                print("content_rating    : {}".format(CONTENT_RATING))
                print("reviews           : {}".format(detail['reviews']))
                print("histogram1        : {}".format(HISTOGRAM[1]))
                print("histogram2        : {}".format(HISTOGRAM[2]))
                print("histogram3        : {}".format(HISTOGRAM[3]))
                print("histogram4        : {}".format(HISTOGRAM[4]))
                print("histogram5        : {}".format(HISTOGRAM[5]))

            # redshiftに詳細データを書き込む
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO superset_schema.app_details (\
                            app_id, \
                            app_name, \
                            platform, \
                            icon_url, \
                            ranking, \
                            ranking_update_at, \
                            rating, \
                            rating_count, \
                            rating_update_at, \
                            genre, \
                            installs, \
                            price, \
                            publisher_id, \
                            publisher_name, \
                            description, \
                            screenshots, \
                            video, \
                            content_rating, \
                            reviews, \
                            histogram1, \
                            histogram2, \
                            histogram3, \
                            histogram4, \
                            histogram5, \
                            is_release, \
                            updated_at, \
                            created_at\
                            ) SELECT \
                            %s, %s, 1, %s, 0, convert_timezone('jst', sysdate), %s, %s, convert_timezone('jst', sysdate), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, convert_timezone('jst', sysdate), convert_timezone('jst', sysdate) where not exists ( \
                            select app_id from superset_schema.app_details where app_id = %s); ",
                            (\
                                    app_id[0], \
                                    json_response['name'], \
                                    json_response['image'], \
                                    RATING, \
                                    RATING_COUNT, \
                                    json_response['applicationCategory'], \
                                    INSTALLS, \
                                    json_response['offers'][0]['price'], \
                                    detail['developer_id'], \
                                    detail['developer'], \
                                    detail_ja['description'], \
                                    SCREENSHOTS, \
                                    detail['video'], \
                                    CONTENT_RATING, \
                                    str(detail['reviews']), \
                                    str(HISTOGRAM[1]), \
                                    str(HISTOGRAM[2]), \
                                    str(HISTOGRAM[3]), \
                                    str(HISTOGRAM[4]), \
                                    str(HISTOGRAM[5]), \
                                    app_id[0]\
                            )\
                    )

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": get_details_aos\n")

