#!/usr/bin/python3.7
# -*- coding: utf8 -*-

import requests
import json
from bs4 import BeautifulSoup
import os
import psycopg2
import psycopg2.extras
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
SLACK_URL=args[5]
DATE=int(args[6])
LOG='/tmp/superset.log'
DEBUG=True
CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=DATE)
print(CHECK_DATE)

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_spend_tt start\n")

def get_dict_resultset(sql):
    print(sql)
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
FILE_ID='1ucsOJTuUp6IFhz99MUVNtFZVeM1RjzkewOdok2X4VQg'

PREV_APP_ID=""
# Redshiftから支出取得
apps_spend=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, store_id, platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, ds.date, Sum(spend) AS spend, Sum(installs) AS installs, Sum(clicks) AS clicks, Sum(impressions) AS impressions, original_currency, Sum(original_spend) AS original_spend FROM (((daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON c.ad_network_id = ad.id) WHERE date = '{}' GROUP BY c.ad_network_id, a.id, a.name, store_id, platform, bundle_id, ad.id, ad.name, ds.date, original_currency".format(str(CHECK_DATE)))
for spend in sorted(sorted(apps_spend, key=lambda x:x['app_id']), key=lambda x:x['bundle_id']):

    # 初回動作チェック
    if PREV_APP_ID == "":
        i = 0
    elif PREV_APP_ID == spend['app_id']:
        i = 1
    else:
        PREV_APP_ID = spend['app_id']
        i = 0
    PREV_APP_ID = spend['app_id']

    # 初期処理
    if i == 0:
        # スプレッドシート名生成
        if spend['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + spend['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + spend['app_name'] + "_Android／シミュレーション"
    
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
            target = worksheet.find(str(CHECK_DATE))
    
        except:
            # 売上集計シートに行追加
            worksheet = gc.open(SPREADSHEET_NAME).worksheet("売上集計")
            # リファレンス行をコピー
            reference_list = worksheet.row_values(11, value_render_option='FORMULA')
            # 最終行にペースト
            print(str(reference_list))
            del reference_list[0]
            worksheet.append_row(reference_list, value_input_option='USER_ENTERED')

            # 集計シート開き直し
            worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")

            # 最終行に日付追加
            worksheet.append_row(['', "{}".format(CHECK_DATE)], value_input_option='USER_ENTERED')
            target = worksheet.find(str(CHECK_DATE))

    sleep(6)

    # 接続確認
    try:
        worksheet.update_cell(target.row, 3, spend['app_name'])
    except Exception as e:
        print("APIError が発生しました")
        # OAuth処理
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('gcp.json', scope)
        gc = gspread.authorize(credentials)

        http = httplib2.Http(timeout=7200)
        http = credentials.authorize(http)
        service = build('drive', 'v3', http=http)
        worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
        worksheet.update_cell(target.row, 3, spend['app_name'])
        print(type(e))
        print(e)

    # spendをドルに戻す
    SPEND=spend['spend']/100
    if DEBUG:
        print(spend['app_name'] + " : " + spend['platform'] + " : " + str(spend['store_id']) + " : " + str(spend['bundle_id']) + " : " + spend['ad_name'] + " : 広告支出 $" + str(SPEND))

    # アプリ名
    worksheet.update_cell(target.row, 3, spend['app_name'])
    # Applovin出稿
    if spend['ad_name'] == 'Applovin':
        worksheet.update_cell(target.row, 18, spend['installs'])
        worksheet.update_cell(target.row, 19, SPEND)

    # Tapjoy出稿
    if spend['ad_name'] == 'Tapjoy':
        worksheet.update_cell(target.row, 20, spend['installs'])
        worksheet.update_cell(target.row, 21, SPEND)

    # Unity出稿
    if spend['ad_name'] == 'Unity':
        worksheet.update_cell(target.row, 22, spend['installs'])
        worksheet.update_cell(target.row, 23, SPEND)

    # Facebook出稿
    if spend['ad_name'] == 'Facebook':
        worksheet.update_cell(target.row, 24, spend['installs'])
        worksheet.update_cell(target.row, 25, SPEND)

    # ironSource出稿
    if spend['ad_name'] == 'ironSource':
        worksheet.update_cell(target.row, 26, spend['installs'])
        worksheet.update_cell(target.row, 27, SPEND)

    # Google Ads出稿
    if spend['ad_name'] == 'Google Ads':
        worksheet.update_cell(target.row, 31, spend['installs'])
        worksheet.update_cell(target.row, 32, SPEND)

    # TikTok出稿
    if spend['ad_name'] == 'TikTok':
        worksheet.update_cell(target.row, 33, spend['installs'])
        worksheet.update_cell(target.row, 34, SPEND)

    # Snapchat出稿
    if spend['ad_name'] == 'Snapchat':
        worksheet.update_cell(target.row, 37, spend['installs'])
        worksheet.update_cell(target.row, 38, SPEND)

    # Mintegral出稿
    if spend['ad_name'] == 'Mintegral':
        worksheet.update_cell(target.row, 39, spend['installs'])
        worksheet.update_cell(target.row, 40, SPEND)

    # Apple Search Ads出稿
    if spend['ad_name'] == 'Apple Search Ads':
        worksheet.update_cell(target.row, 41, spend['installs'])
        worksheet.update_cell(target.row, 42, SPEND)

    # Maio出稿
    if spend['ad_name'] == 'Maio':
        worksheet.update_cell(target.row, 43, spend['installs'])
        worksheet.update_cell(target.row, 44, SPEND)

    # i-mobile Affiliate出稿
    if spend['ad_name'] == 'i-mobile Affiliate':
        worksheet.update_cell(target.row, 45, spend['installs'])
        worksheet.update_cell(target.row, 46, SPEND)

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_spend_tt end\n")

