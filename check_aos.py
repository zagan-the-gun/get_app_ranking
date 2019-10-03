#!/usr/bin/python3.7
# -*- coding: utf8 -*-

import requests
import json
from bs4 import BeautifulSoup
import os
import psycopg2
import psycopg2.extras
import play_scraper
import datetime
import pytz
import sys

args = sys.argv
jst = pytz.timezone('Asia/Tokyo')

DATABASE_URL='postgresql://'+ args[1] + ':' + args[2] + '@'+ args[3] + ':5439/'+ args[4]
LOG='/tmp/superset.log'
DEBUG=True
SLACK_URL=args[5]

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def get_dict_resultset(sql):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute (sql)
            results = cur.fetchall()
            dict_result = []
            for row in results:
                dict_result.append(dict(row))
            return dict_result

# redshiftからapp_idを取得する
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT app_id, is_release FROM superset_schema.app_details WHERE platform = 1;")

        for app_id in cur:

            # google playからページ取得
            target_url = "https://play.google.com/store/apps/details?id={app_id}".format(app_id=app_id[0])
            response = requests.get(target_url) #requestsを使って、webから取得

            # スクレイピング１
            r = requests.get(target_url)
            soup = BeautifulSoup(r.text, 'lxml') #要素を抽出

            if app_id[1] == True:
                # 生存から死亡
                if soup.find('script', type="application/ld+json") is None:

                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE superset_schema.app_details SET is_release = FALSE, updated_at = convert_timezone('jst', sysdate) WHERE app_id = '{}'".format(app_id[0]))

                    # 死亡アプリの詳細取得
                    app = get_dict_resultset("SELECT * FROM superset_schema.app_details WHERE app_id = '{}'".format(app_id[0]))

                    SCREENSHOTS=""
                    if app[0]['screenshots'] is not None:
                        for i,s in enumerate(json.loads(app[0]['screenshots'])):
                            SCREENSHOTS=SCREENSHOTS + "<" + s + "|" + str(i) + "> "

                    text="{icon_url}\n<https://play.google.com/store/apps/details?id={app_id} | {app_name}> の android アプリが非公開になりました\nジャンル: {genre}\nコンテンツレーティング: {content_rating}\nデベロッパー: <https://play.google.com/store/apps/developer?id={publisher_id} | {publisher_name}>\nインストール数: {installs}\nスクリーンショット: {screenshots}\nビデオ: {video}\n"\
                            .format(icon_url=app[0]['icon_url'], app_id=app[0]['app_id'], app_name=app[0]['app_name'], genre=app[0]['genre'], content_rating=app[0]['content_rating'], publisher_id=app[0]['publisher_id'], publisher_name=app[0]['publisher_name'], rating=app[0]['rating'], rating_count=app[0]['rating_count'], reviews=app[0]['reviews'], description=app[0]['description'], screenshots=SCREENSHOTS, video=app[0]['video'], installs=app[0]['installs'])

                    # Slackに死亡を報告
                    requests.post("https://hooks.slack.com/services/" + SLACK_URL, data = json.dumps({
                        'text': text,  #通知内容
                        'username': u'確認',  #ユーザー名
                        'icon_emoji': u':smile_cat:',  #アイコン
                        'link_names': 1,  #名前をリンク化
                    }))

                    if DEBUG:
                        print("app is dead. {}".format(app_id[0]))

                # 生存から生存
                else:
                    if DEBUG:
                        print("app is alive, no changes. {}".format(app_id[0]))
 
            else:
                # 死亡から死亡
                if soup.find('script', type="application/ld+json") is None:
                    if DEBUG:
                        print("app is dead, no changes. {}".format(app_id[0]))

                # 死亡から生存
                else:

                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE superset_schema.app_details SET is_release = TRUE, updated_at = convert_timezone('jst', sysdate) WHERE app_id = '{}'".format(app_id[0]))

                    # 生存アプリの詳細取得
                    app = get_dict_resultset("SELECT * FROM superset_schema.app_details WHERE app_id = '{}'".format(app_id[0]))

                    REVIVAL_DATE=(app[0]['updated_at'] - datetime.datetime.now(tz=jst)).days

                    SCREENSHOTS=""
                    if app[0]['screenshots'] is not None:
                        for i,s in enumerate(json.loads(app[0]['screenshots'])):
                            SCREENSHOTS=SCREENSHOTS + "<" + s + "|" + str(i) + "> "

                    text="{icon_url}\n<https://play.google.com/store/apps/details?id={app_id} | {app_name}> の android アプリが再公開になりました\n{revival_date} 日ぶり\nジャンル: {genre}\nコンテンツレーティング: {content_rating}\nデベロッパー: <https://play.google.com/store/apps/developer?id={publisher_id} | {publisher_name}>\nインストール数: {installs}\nスクリーンショット: {screenshots}\nビデオ: {video}\n"\
                            .format(icon_url=app[0]['icon_url'], app_id=app[0]['app_id'], app_name=app[0]['app_name'], revival_date=REVIVAL_DATE, genre=app[0]['genre'], content_rating=app[0]['content_rating'], publisher_id=app[0]['publisher_id'], publisher_name=app[0]['publisher_name'], rating=app[0]['rating'], rating_count=app[0]['rating_count'], reviews=app[0]['reviews'], description=app[0]['description'], screenshots=SCREENSHOTS, video=app[0]['video'], installs=app[0]['installs'])

                    # Slackに生存を報告
                    requests.post("https://hooks.slack.com/services/" + SLACK_URL, data = json.dumps({
                        'text': text,  #通知内容
                        'username': u'確認',  #ユーザー名
                        'icon_emoji': u':smile_cat:',  #アイコン
                        'link_names': 1,  #名前をリンク化
                    }))

                    if DEBUG:
                        print("app is alive. {}".format(app_id[0]))

