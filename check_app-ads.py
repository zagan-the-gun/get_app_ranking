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

DEBUG=True
SLACK_URL=args[1]

# 配列
APP_ADDS=[
        'https://maskapp.club/app-ads.txt',
        'https://babangida.be/app-ads.txt',
        'https://bluebirdstudio.net/app-ads.txt',
        'https://ftyapp.com/app-ads.txt',
        'https://manimani.be/app-ads.txt',
        'https://novelapps.be/app-ads.txt',
        'https://imikowaapps.be/app-ads.txt',
        'https://spiritduck.be/app-ads.txt',
        'https://aslmoco.be/app-ads.txt',
        'https://colaup.be/app-ads.txt'
        ]

# app-ads.txt取得
for target_url in APP_ADDS:
    response = requests.get(target_url, timeout=(10.0, 10.0))
    if response.status_code != requests.codes.ok:
        # slackに通知
        text="URLアクセスエラー {target_url}\nステータスコード: {status_code}"\
                .format(target_url=target_url, status_code=response.status_code)

        requests.post("https://hooks.slack.com/services/" + SLACK_URL, data = json.dumps({
            'text': text,  #通知内容
#            'username': u'アクセスエラー',  #ユーザー名
#            'icon_emoji': u':smile_cat:',  #アイコン
            'link_names': 1,  #名前をリンク化
        }))

