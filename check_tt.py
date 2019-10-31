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

import gspread
import json

from oauth2client.service_account import ServiceAccountCredentials

#from apiclient.discovery import build
from googleapiclient.discovery import build
import httplib2

from time import sleep

args = sys.argv
jst = pytz.timezone('Asia/Tokyo')

DATABASE_URL='postgresql://'+ args[1] + ':' + args[2] + '@'+ args[3] + ':5439/'+ args[4]
LOG='/tmp/superset.log'
DEBUG=True
SLACK_URL=args[5]
TODAY=datetime.date.today()

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

# OAuth処理
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('gcp.json', scope)

http = httplib2.Http()
http = credentials.authorize(http)
service = build('drive', 'v3', http=http)

gc = gspread.authorize(credentials)
FILE_ID='1dz_3Q82N6zZtzylK7cdthWUCklnC-b1IHNnPKllcxTg'

# redshiftからapp_idを取得する
"""
with get_connection() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
#        cur.execute("SELECT * FROM ((daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) WHERE date = CURRENT_DATE ")
        cur.execute("SELECT a.name, c.name, ds.date FROM ((daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) WHERE date = CURRENT_DATE + INTERVAL '-1 day'")
        for daily_spend in cur:
            print(daily_spend)
"""

# Redshiftから支出取得
#apps=get_dict_resultset("SELECT * FROM ((daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) WHERE date = CURRENT_DATE + INTERVAL '-1 day'")
#apps_spend=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, platform, c.id AS campaign_id, c.name AS campaign_name, ds.date, spend, installs, clicks, impressions, original_currency, original_spend FROM ((daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) WHERE date = CURRENT_DATE + INTERVAL '-1 day'")
#apps_spend=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, store_id, platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, c.id AS campaign_id, c.name AS campaign_name, ds.date, spend, installs, clicks, impressions, original_currency, original_spend FROM (((daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON c.ad_network_id = ad.id) WHERE date = CURRENT_DATE + INTERVAL '-1 day'")
#apps_spend=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, store_id, platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, c.id AS campaign_id, c.name AS campaign_name, ds.date, spend, installs, clicks, impressions, original_currency, original_spend FROM (((daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON c.ad_network_id = ad.id) WHERE date = CURRENT_DATE + INTERVAL '-1 day'")
apps_spend=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, store_id, platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, ds.date, Sum(spend) AS spend, Sum(installs) AS installs, Sum(clicks) AS clicks, Sum(impressions) AS impressions, original_currency, Sum(original_spend) AS original_spend FROM (((daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON c.ad_network_id = ad.id) WHERE date = CURRENT_DATE + INTERVAL '-1 day' GROUP BY c.ad_network_id, a.id, a.name, store_id, platform, bundle_id, ad.id, ad.name, ds.date, original_currency")
#apps_spend=get_dict_resultset("SELECT app_id, Sum(spend) AS spend FROM (daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) WHERE date = CURRENT_DATE + INTERVAL '-1 day' GROUP BY c.ad_network_id, app_id")

#print(apps_spend)
#for app in sorted(apps_spend, key=lambda x:x['app_id']):
for spend in sorted(sorted(apps_spend, key=lambda x:x['app_id']), key=lambda x:x['bundle_id']):
#for app in sorted(sorted(apps_spend, key=lambda x:x['app_id']), key=lambda x:x['name']):
#for app in apps_spend:
#    print(app)
    print(spend['app_name'] + " : " + spend['platform'] + " : " + str(spend['store_id']) + " : " + str(spend['bundle_id']) + " : " + spend['ad_name'] + " : 広告支出 " + str(spend['spend']) + "円")

    if spend['platform'] != 'android':
        SPREADSHEET_NAME=spend['app_name'] + "／シミュレーション"
    else:
        SPREADSHEET_NAME=spend['app_name'] + "_Android／シミュレーション"

    # Googleスプレッドシート無ければ作成
    try:
        worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
    except:
        new_file_body = {
            'name': SPREADSHEET_NAME,  # 新しいファイルのファイル名. 省略も可能
            'parents': ['1Z6nHs-LoO8D_HdXuY2wkH5yd2Uh70daP'],  # Copy先のFolder ID. 省略も可能
            'type': 'user',
            'role': 'owner',
            'emailAddress': 'ishizuka@tokyo-tsushin.com',
        }
    
        new_file = service.files().copy(
            fileId=FILE_ID, body=new_file_body
        ).execute()
    
        worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")

    # 当日行取得、無ければ作る
    try:
        target = worksheet.find(str(TODAY))

    except:
        last_row = worksheet.append_row(["{}".format(TODAY)], value_input_option='USER_ENTERED')

    target = worksheet.find(str(TODAY))

    sleep(3)

    # アプリ名
    worksheet.update_cell(target.row, 3, spend['app_name'])
    # Applovin出稿
    if spend['ad_name'] == 'Applovin':
        worksheet.update_cell(target.row, 17, spend['installs'])
        worksheet.update_cell(target.row, 18, spend['spend'])

    # Tapjoy出稿
    if spend['ad_name'] == 'Tapjoy':
        worksheet.update_cell(target.row, 19, spend['installs'])
        worksheet.update_cell(target.row, 20, spend['spend'])

    # Unity出稿
    if spend['ad_name'] == 'Unity':
        worksheet.update_cell(target.row, 21, spend['installs'])
        worksheet.update_cell(target.row, 22, spend['spend'])

    # Facebook出稿
    if spend['ad_name'] == 'Facebook':
        worksheet.update_cell(target.row, 23, spend['installs'])
        worksheet.update_cell(target.row, 24, spend['spend'])

    # ironSource出稿
    if spend['ad_name'] == 'ironSource':
        worksheet.update_cell(target.row, 25, spend['installs'])
        worksheet.update_cell(target.row, 26, spend['spend'])

    # TikTok出稿
    if spend['ad_name'] == 'TikTok':
        worksheet.update_cell(target.row, 32, spend['installs'])
        worksheet.update_cell(target.row, 33, spend['spend'])

    # Snapchat出稿
    if spend['ad_name'] == 'Snapchat':
        worksheet.update_cell(target.row, 36, spend['installs'])
        worksheet.update_cell(target.row, 37, spend['spend'])

    # Mintegral出稿
    if spend['ad_name'] == 'Mintegral':
        worksheet.update_cell(target.row, 38, spend['installs'])
        worksheet.update_cell(target.row, 39, spend['spend'])


# Redshiftから収入取得
apps_revenue=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, store_id, platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, dar.date, Sum(revenue) AS revenue, Sum(impressions) AS impressions, Sum(clicks) AS clicks, Sum(conversions) AS conversions FROM (((daily_ad_revenue dar LEFT OUTER JOIN publisher_apps pa ON dar.publisher_app_id = pa.id) LEFT OUTER JOIN apps a ON pa.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON pa.ad_network_id = ad.id) WHERE date = CURRENT_DATE + INTERVAL '-1 day' GROUP BY pa.ad_network_id, a.id, a.name, store_id, platform, bundle_id, ad.id, ad.name, dar.date")

for revenue in sorted(sorted(apps_revenue, key=lambda x:x['app_id']), key=lambda x:x['bundle_id']):
    print(revenue['app_name'] + " : " + revenue['platform'] + " : " + str(revenue['store_id']) + " : " + str(revenue['bundle_id']) + " : " + revenue['ad_name'] + " : 広告収入 " + str(revenue['revenue']) + "円")

    if revenue['platform'] != 'android':
        SPREADSHEET_NAME=revenue['app_name'] + "／シミュレーション"
    else:
        SPREADSHEET_NAME=revenue['app_name'] + "_Android／シミュレーション"

    # Googleスプレッドシート無ければ作成
    try:
        worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
    except:
        new_file_body = {
            'name': SPREADSHEET_NAME,  # 新しいファイルのファイル名. 省略も可能
            'parents': ['1Z6nHs-LoO8D_HdXuY2wkH5yd2Uh70daP'],  # Copy先のFolder ID. 省略も可能
            'type': 'user',
            'role': 'owner',
            'emailAddress': 'ishizuka@tokyo-tsushin.com',
        }
    
        new_file = service.files().copy(
            fileId=FILE_ID, body=new_file_body
        ).execute()
    
        worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")

    # 当日行取得、無ければ作る
    try:
        target = worksheet.find(str(TODAY))

    except:
        last_row = worksheet.append_row(["{}".format(TODAY)], value_input_option='USER_ENTERED')

    target = worksheet.find(str(TODAY))

    sleep(3)

    # アプリ名
#    worksheet.update_cell(target.row, 3, revenue['app_name'])
    # Unity Ads出稿
    if revenue['ad_name'] == 'Unity Ads':
        worksheet.update_cell(target.row, 6, revenue['revenue'])

    # Applovin出稿
    if revenue['ad_name'] == 'Applovin':
        worksheet.update_cell(target.row, 7, revenue['revenue'])

    # Ad Generation出稿
    if revenue['ad_name'] == 'Ad Generation':
        worksheet.update_cell(target.row, 9, revenue['revenue'])

    # FIVE出稿
    if revenue['ad_name'] == 'FIVE':
        worksheet.update_cell(target.row, 10, revenue['revenue'])

    # ironSource-Publisher出稿
    if revenue['ad_name'] == 'ironSource-Publisher':
        worksheet.update_cell(target.row, 14, revenue['revenue'])

    # Tapjoy出稿
    if revenue['ad_name'] == 'Tapjoy':
        worksheet.update_cell(target.row, 15, revenue['revenue'])

    # Facebook Audience Network出稿
    if revenue['ad_name'] == 'Facebook Audience Network':
        worksheet.update_cell(target.row, 16, revenue['revenue'])

    # Vungle出稿
    if revenue['ad_name'] == 'Vungle':
        worksheet.update_cell(target.row, 29, revenue['revenue'])

    # TikTok Audience Network出稿
    if revenue['ad_name'] == 'TikTok Audience Network':
        worksheet.update_cell(target.row, 37, revenue['revenue'])

    # Mintegral Publisher出稿
    if revenue['ad_name'] == 'Mintegral Publisher':
        worksheet.update_cell(target.row, 38, revenue['revenue'])

#apps=get_dict_resultset("SELECT * FROM apps")
#for app in apps:
#    print(app['name'])

