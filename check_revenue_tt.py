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
#TODAY=datetime.date.today()
CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=DATE)

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_tt start\n")

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

PREV_APP_ID=""
# Redshiftから収入取得
apps_revenue=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, store_id, platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, dar.date, Sum(revenue) AS revenue, Sum(impressions) AS impressions, Sum(clicks) AS clicks, Sum(conversions) AS conversions FROM (((daily_ad_revenue dar LEFT OUTER JOIN publisher_apps pa ON dar.publisher_app_id = pa.id) LEFT OUTER JOIN apps a ON pa.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON pa.ad_network_id = ad.id) WHERE date = '{}' GROUP BY pa.ad_network_id, a.id, a.name, store_id, platform, bundle_id, ad.id, ad.name, dar.date".format(str(CHECK_DATE)))

for revenue in sorted(sorted(apps_revenue, key=lambda x:x['app_id']), key=lambda x:x['bundle_id']):

    # 初回動作チェック
    if PREV_APP_ID == "":
        i = 0
    elif PREV_APP_ID == revenue['app_id']:
        i = 1
    else:
        PREV_APP_ID = revenue['app_id']
        i = 0

    PREV_APP_ID = revenue['app_id']

    # 初期処理
    if i == 0:

        if revenue['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + revenue['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + revenue['app_name'] + "_Android／シミュレーション"
    
        if DEBUG:
            print(str(CHECK_DATE) + ": check_revenue_tt : " + SPREADSHEET_NAME)
    
        # Googleスプレッドシート無ければ作成
        try:
            gc = gspread.authorize(credentials)
            worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
            print("ファイルオープン成功")
        except:
            new_file_body = {
                'name': SPREADSHEET_NAME,  # 新しいファイルのファイル名. 省略も可能
                'parents': ['1Z6nHs-LoO8D_HdXuY2wkH5yd2Uh70daP'],  # Copy先のFolder ID. 省略も可能
                'type': 'user',
                'role': 'owner',
                'emailAddress': 'ishizuka@tokyo-tsushin.com',
            }
        
            print("ファイル作成")
            print(FILE_ID)
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
            target = worksheet.append_row(['', "{}".format(CHECK_DATE)], value_input_option='USER_ENTERED')
            target = worksheet.find(str(CHECK_DATE))

    sleep(4)
    # アプリ名記入して接続確認
    try:
        worksheet.update_cell(target.row, 3, revenue['app_name'])
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
        worksheet.update_cell(target.row, 3, revenue['app_name'])
        print(type(e))
        print(e)

    # revenueをドルに戻す
    REVENUE=revenue['revenue']/100
    if DEBUG:
        print(revenue['app_name'] + " : " + revenue['platform'] + " : " + revenue['ad_name'] + " : " + " : 広告収入 $" + str(REVENUE))

    # Unity Ads出稿
    if revenue['ad_name'] == 'Unity Ads':
        worksheet.update_cell(target.row, 6, REVENUE)

    # nend出稿
#    if revenue['ad_name'] == '':
#        worksheet.update_cell(target.row, 9, REVENUE)

    # Ad Generation出稿
    elif revenue['ad_name'] == 'Ad Generation':
        worksheet.update_cell(target.row, 10, REVENUE)

    # FIVE出稿
    elif revenue['ad_name'] == 'FIVE':
        worksheet.update_cell(target.row, 11, REVENUE)

    # i-mobile Affiliate出稿
    elif revenue['ad_name'] == 'i-mobile Affiliate':
        worksheet.update_cell(target.row, 13, REVENUE)

    # ironSource-Publisher出稿
    elif revenue['ad_name'] == 'ironSource-Publisher':
        worksheet.update_cell(target.row, 15, REVENUE)

    # Tapjoy出稿
    elif revenue['ad_name'] == 'Tapjoy':
        worksheet.update_cell(target.row, 16, REVENUE)

    # Facebook Audience Network出稿
    elif revenue['ad_name'] == 'Facebook Audience Network':
        worksheet.update_cell(target.row, 17, REVENUE)

    # Vungle出稿
    elif revenue['ad_name'] == 'Vungle':
        worksheet.update_cell(target.row, 30, REVENUE)

    # TikTok Audience Network出稿
    elif revenue['ad_name'] == 'TikTok Audience Network':
        worksheet.update_cell(target.row, 35, REVENUE)

    # Mintegral Publisher出稿
    elif revenue['ad_name'] == 'Mintegral Publisher':
        worksheet.update_cell(target.row, 36, REVENUE)

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_tt end\n")

