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

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_detail_tt start\n")

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
gc = gspread.authorize(credentials)

http = httplib2.Http(timeout=7200)
http = credentials.authorize(http)
service = build('drive', 'v3', http=http)

FILE_ID='1ucsOJTuUp6IFhz99MUVNtFZVeM1RjzkewOdok2X4VQg'

# Redshiftから収入取得
apps_pa_revenue=get_dict_resultset("SELECT pa.name AS pa_name, a.id AS app_id, a.name AS app_name, platform, bundle_id, ad.name AS ad_name, Sum(revenue) AS revenue FROM (((daily_ad_revenue dar LEFT OUTER JOIN publisher_apps pa ON dar.publisher_app_id = pa.id) LEFT OUTER JOIN apps a ON pa.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON pa.ad_network_id = ad.id) WHERE date = '{}' GROUP BY pa.ad_network_id, pa.name, a.id, a.name, store_id, platform, bundle_id, ad.name".format(str(CHECK_DATE)))

PREV_APP_ID=""
for pa_revenue in sorted(sorted(apps_pa_revenue, key=lambda x:x['app_id']), key=lambda x:x['bundle_id']):

    # 初回動作チェック
    if PREV_APP_ID == "":
        i = 0
    elif PREV_APP_ID == pa_revenue['app_id']:
        i = 1
    else:
        PREV_APP_ID = pa_revenue['app_id']
        i = 0
    PREV_APP_ID = pa_revenue['app_id']

    # 初期処理
    if i == 0:
        # スプレッドシート名生成
        if pa_revenue['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + pa_revenue['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + pa_revenue['app_name'] + "_Android／シミュレーション"
    
        if DEBUG:
            print(str(CHECK_DATE) + ": check_revenue_tt : " + SPREADSHEET_NAME)
    
        # Googleスプレッドシート開く、無ければ作成
        try:
            gc = gspread.authorize(credentials)
            worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
        except:
            print("except が発生しました")
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
            gc = gspread.authorize(credentials)
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

        # リセット&数字を入れておく
        try:
            sleep(1)
            worksheet.update_cell(target.row, 4, 0)
            sleep(1)
            worksheet.update_cell(target.row, 5, 0)
            sleep(1)
            worksheet.update_cell(target.row, 7, 0)
            sleep(1)
            worksheet.update_cell(target.row, 8, 0)
        except:
            sleep(5)
            worksheet.update_cell(target.row, 8, 0)
            sleep(2)
            worksheet.update_cell(target.row, 7, 0)
            sleep(2)
            worksheet.update_cell(target.row, 5, 0)
            sleep(2)
            worksheet.update_cell(target.row, 4, 0)

    sleep(5)

    # 接続確認
    try:
        worksheet.update_cell(target.row, 3, pa_revenue['app_name'])
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
        worksheet.update_cell(target.row, 3, pa_revenue['app_name'])
        print(type(e))
        print(e)

    # revenueをドルに戻す
    REVENUE=pa_revenue['revenue']/100

    # Google AdMob出稿
    if 'Google AdMob' in pa_revenue['ad_name']:
        if DEBUG:
            print(pa_revenue['app_name'] + " : " + pa_revenue['platform'] + " : " + str(pa_revenue['bundle_id']) + " : " + pa_revenue['ad_name'] + " : 広告収入 $" + str(REVENUE))
        COL_BANNER=4
        COL_INTERSTITIAL=5
        if 'バナー' in pa_revenue['pa_name'] or 'Banner' in pa_revenue['pa_name'] or 'banner' in pa_revenue['pa_name'] or 'レクタングル' in pa_revenue['pa_name'] or 'Rectangle' in pa_revenue['pa_name'] or 'rectangle' in pa_revenue['pa_name']:
            worksheet.update_cell(target.row, COL_BANNER, float(worksheet.cell(target.row, COL_BANNER).value) + float(REVENUE))

        else:
            worksheet.update_cell(target.row, COL_INTERSTITIAL, float(worksheet.cell(target.row, COL_INTERSTITIAL).value) + float(REVENUE))

    # Applovin出稿
    elif 'Applovin' in pa_revenue['ad_name']:
        if DEBUG:
            print(pa_revenue['app_name'] + " : " + pa_revenue['platform'] + " : " + str(pa_revenue['bundle_id']) + " : " + pa_revenue['ad_name'] + " : 広告収入 $" + str(REVENUE))
        COL_BANNER=7
        COL_INTERSTITIAL=8
#        if 'バナー' in pa_revenue['pa_name']:
        if 'バナー' in pa_revenue['pa_name'] or 'Banner' in pa_revenue['pa_name'] or 'banner' in pa_revenue['pa_name'] or 'レクタングル' in pa_revenue['pa_name'] or 'Rectangle' in pa_revenue['pa_name'] or 'rectangle' in pa_revenue['pa_name']:
            worksheet.update_cell(target.row, COL_BANNER, float(worksheet.cell(target.row, COL_BANNER).value) + float(REVENUE))

        else:
            worksheet.update_cell(target.row, COL_INTERSTITIAL, float(worksheet.cell(target.row, COL_INTERSTITIAL).value) + float(REVENUE))

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_detail_tt end\n")

