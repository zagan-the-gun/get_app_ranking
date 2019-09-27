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
DEBUG=True

def get_connection():
    return psycopg2.connect(DATABASE_URL)

# redshiftからランキング更新前のapp_idを取得する
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT app_id FROM superset_schema.app_details WHERE platform = 2 AND rating_update_at < current_date;")
        for app_id in cur:

            # iTunesからページ取得
            target_url = "https://itunes.apple.com/lookup?id={app_id}&country=JP".format(app_id=app_id[0])
            print(target_url)
            response = requests.get(target_url) #requestsを使って、webから取得
            app_dict = json.loads(response.text)
            print(app_dict)

            if app_dict['resultCount'] == 0:
                target_url = "https://itunes.apple.com/lookup?id={app_id}".format(app_id=app_id[0])
                response = requests.get(target_url) #requestsを使って、webから取得
                app_dict = json.loads(response.text)

                if app_dict['resultCount'] == 0:
                    target_url = "https://itunes.apple.com/lookup?id={app_id}&country=CN".format(app_id=app_id[0])
                    response = requests.get(target_url) #requestsを使って、webから取得
                    app_dict = json.loads(response.text)

            # レーティングチェック
            # 誰にも評価されてないと空っぽ
            print(app_dict)
            if app_dict['results'][0].get('averageUserRatingForCurrentVersion') is not None:
                RATING = app_dict['results'][0]['averageUserRatingForCurrentVersion']
                RATING_COUNT = app_dict['results'][0]['userRatingCountForCurrentVersion']
            else:
                RATING = 0
                RATING_COUNT = 0

            # スクリーンショットArray化
            SCREENSHOTS=[]
            if app_dict['results'][0]['screenshotUrls'] is not None:
                for s in app_dict['results'][0]['screenshotUrls']:
                    SCREENSHOTS.append(s)

            if DEBUG:
                print("----------------------")
                print("app_id            : {}".format(app_dict['results'][0]['trackId']))
                print("app_name          : {}".format(app_dict['results'][0]['trackCensoredName']))
                print("platform          : 2")
                print("icon_url          : {}".format(app_dict['results'][0]['artworkUrl512']))
                print("ranking           : -")
                print("ranking_update_at : -")
                print("rating            : {}".format(RATING))
                print("rating_count      : {}".format(RATING_COUNT))
                print("rating_update_at  : -now-")
                print("genere            : {}".format(app_dict['results'][0]['primaryGenreName']))
                print("installs          : {}".format(RATING_COUNT))
                print("price             : {}".format(app_dict['results'][0]['price']))
                print("publisher_id      : {}".format(app_dict['results'][0]['artistId']))
                print("publisher_name    : {}".format(app_dict['results'][0]['artistName']))
                print("release_at        : {}".format(app_dict['results'][0]['releaseDate']))
                print("description       : {}".format(app_dict['results'][0]['description']))
                print("screenshots       : {}".format(str(SCREENSHOTS).replace("\'", "\"")))
                print("video             : -")
                print("content_rating    : {}".format(app_dict['results'][0]['trackContentRating']))
                print("reviews           : {}".format(0))
                print("histogram1        : {}".format(0))
                print("histogram2        : {}".format(0))
                print("histogram3        : {}".format(0))
                print("histogram4        : {}".format(0))
                print("histogram5        : {}".format(0))

            # redshiftに詳細データの更新を書き込む
            with get_connection() as conn:
                with conn.cursor() as cur:
                    # 詳細データ更新
                    cur.execute("UPDATE superset_schema.app_details SET \
                            app_name = %s, \
                            icon_url = %s, \
                            rating = %s, \
                            rating_count = %s, \
                            rating_update_at = convert_timezone('jst', sysdate), \
                            genre = %s, \
                            installs = %s, \
                            price = %s, \
                            publisher_id = %s, \
                            publisher_name = %s, \
                            release_at = %s, \
                            description = %s, \
                            screenshots = %s, \
                            video = %s, \
                            content_rating = %s, \
                            reviews = %s, \
                            histogram1 = %s, \
                            histogram2 = %s, \
                            histogram3 = %s, \
                            histogram4 = %s, \
                            histogram5 = %s, \
                            is_release = TRUE, \
                            updated_at = convert_timezone('jst', sysdate) \
                            WHERE app_id = %s;",
                            (\
                                    app_dict['results'][0]['trackCensoredName'], \
                                    app_dict['results'][0]['artworkUrl512'], \
                                    RATING, \
                                    RATING_COUNT, \
                                    app_dict['results'][0]['primaryGenreName'], \
                                    RATING_COUNT, \
                                    app_dict['results'][0]['price'], \
                                    app_dict['results'][0]['artistId'], \
                                    app_dict['results'][0]['artistName'], \
                                    app_dict['results'][0]['releaseDate'],\
                                    app_dict['results'][0]['description'], \
                                    str(SCREENSHOTS).replace("\'", "\""), \
                                    "", \
                                    app_dict['results'][0]['trackContentRating'], \
                                    '0', \
                                    '0', \
                                    '0', \
                                    '0', \
                                    '0', \
                                    '0', \
                                    app_dict['results'][0]['trackId']))

                    # ランキングデータ追加
                    cur.execute("INSERT INTO superset_schema.app_rankings (\
                            app_id, \
                            app_name, \
                            platform, \
                            icon_url, \
                            ranking, \
                            rating, \
                            rating_count, \
                            genre, \
                            installs, \
                            price, \
                            publisher_id, \
                            publisher_name, \
                            reviews, \
                            histogram1, \
                            histogram2, \
                            histogram3, \
                            histogram4, \
                            histogram5, \
                            created_at\
                            ) SELECT %s, %s, 2, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, convert_timezone('jst', sysdate); ",
                            (\
                                    app_dict['results'][0]['trackId'], \
                                    app_dict['results'][0]['trackCensoredName'], \
                                    app_dict['results'][0]['artworkUrl512'], \
                                    RATING, \
                                    RATING_COUNT, \
                                    app_dict['results'][0]['primaryGenreName'], \
                                    RATING_COUNT, \
                                    app_dict['results'][0]['price'], \
                                    app_dict['results'][0]['artistId'], \
                                    app_dict['results'][0]['artistName'], \
                                    '0', \
                                    '0', \
                                    '0', \
                                    '0', \
                                    '0', \
                                    '0'\
                                    ))

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": get_ranking_ios\n")

