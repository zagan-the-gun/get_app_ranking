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
SLACK_URL=args[5]

def get_connection():
    return psycopg2.connect(DATABASE_URL)

# redshiftからapp_idリストを取得 joinで不足分だけに変更する
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT app_id FROM superset_schema.app_ids WHERE platform = 2  EXCEPT SELECT app_id FROM superset_schema.app_details WHERE platform = 2;")
        for app_id in cur:

            # iTunesからページ取得
            target_url = "https://itunes.apple.com/lookup?id={app_id}&country=JP".format(app_id=app_id[0])
            response = requests.get(target_url) #requestsを使って、webから取得
            app_dict = json.loads(response.text)

            if app_dict['resultCount'] == 0:
                target_url = "https://itunes.apple.com/lookup?id={app_id}".format(app_id=app_id[0])
                response = requests.get(target_url) #requestsを使って、webから取得
                app_dict = json.loads(response.text)

                if app_dict['resultCount'] == 0:
                    target_url = "https://itunes.apple.com/lookup?id={app_id}&country=CN".format(app_id=app_id[0])
                    response = requests.get(target_url) #requestsを使って、webから取得
                    app_dict = json.loads(response.text)

            # レーティングチェック
            # 誰にも評価されてないと空っぽ?
            if app_dict['results'][0].get('averageUserRatingForCurrentVersion') is not None:
                RATING = app_dict['results'][0]['averageUserRatingForCurrentVersion']
            else:
                RATING = 0

            if app_dict['results'][0].get('userRatingCountForCurrentVersion') is not None:
                RATING_COUNT = app_dict['results'][0]['userRatingCountForCurrentVersion']
            else:
                RATING_COUNT = 0

            # プライスチェック
            if app_dict['results'][0].get('price') is not None:
                PRICE = app_dict['results'][0]['price']
            else:
                PRICE = 0

            # スクリーンショットjson化
            SCREENSHOTS=[]
            if app_dict['results'][0]['screenshotUrls'] is not None:
                for s in app_dict['results'][0]['screenshotUrls']:
                    SCREENSHOTS.append(s)

            if DEBUG:
                print("----------------------")
                print("app_id            : {}".format(app_dict['results'][0]['trackId']))
                print("app_name          : {}".format(app_dict['results'][0]['trackName']))
                print("platform          : 2")
                print("icon_url          : {}".format(app_dict['results'][0]['artworkUrl512']))
                print("ranking           : -")
                print("ranking_update_at : -")
                print("rating            : {}".format(RATING))
                print("rating_count      : {}".format(RATING_COUNT))
                print("rating_update_at  : -now-")
                print("genere            : {}".format(app_dict['results'][0]['primaryGenreName']))
                print("installs          : {}".format(RATING_COUNT))
                print("price             : {}".format(PRICE))
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

            # slackに通知
#            text="{icon_url}\n<{track_view_url}&l=0 | {app_name}> の iPhone アプリが追加されました"\
#                    .format(icon_url=app_dict['results'][0]['artworkUrl512'], track_view_url=app_dict['results'][0]['trackViewUrl'], app_name=app_dict['results'][0]['trackCensoredName'])

#            requests.post("https://hooks.slack.com/services/" + SLACK_URL, data = json.dumps({
#                'text': text,  #通知内容
#                'username': u'新規追加',  #ユーザー名
#                'icon_emoji': u':smile_cat:',  #アイコン
#                'link_names': 1,  #名前をリンク化
#            }))

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
                            %s, %s, 2, %s, 0, convert_timezone('jst', sysdate), %s, %s, convert_timezone('jst', sysdate), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, convert_timezone('jst', sysdate), convert_timezone('jst', sysdate) where not exists ( \
                            select app_id from superset_schema.app_details where app_id = %s); ",
                            (\
                                    app_dict['results'][0]['trackId'], \
                                    app_dict['results'][0]['trackName'], \
                                    app_dict['results'][0]['artworkUrl512'], \
                                    RATING, \
                                    RATING_COUNT, \
                                    app_dict['results'][0]['primaryGenreName'], \
                                    RATING_COUNT, \
                                    PRICE, \
                                    app_dict['results'][0]['artistId'], \
                                    app_dict['results'][0]['artistName'], \
                                    app_dict['results'][0]['description'], \
                                    str(SCREENSHOTS).replace("\'", "\""), \
                                    "", \
                                    app_dict['results'][0]['trackContentRating'], \
                                    str(0), \
                                    str(0), \
                                    str(0), \
                                    str(0), \
                                    str(0), \
                                    str(0), \
                                    app_dict['results'][0]['trackId']\
                            )\
                    )

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": get_details_ios\n")

