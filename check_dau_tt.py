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
print(CHECK_DATE)

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt start\n")

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
FILE_ID='1ucsOJTuUp6IFhz99MUVNtFZVeM1RjzkewOdok2X4VQg'

# RedshiftからDAU取得
PREV_APP_ID=""
apps_events=get_dict_resultset("SELECT a.bundle_id AS bundle_id, a.id AS app_id, a.name AS app_name, rm.platform AS platform, Sum(daily_active_users) AS daily_active_users, Sum(tracked_installs) AS tracked_installs FROM (reporting_metrics rm LEFT OUTER JOIN apps a ON rm.app_id = a.id) WHERE date = '{}' GROUP BY rm.app_id, rm.platform, a.name, a.id, a.bundle_id".format(str(CHECK_DATE)))
for event in sorted(sorted(apps_events, key=lambda x:x['app_id'] or ""), key=lambda x:x['bundle_id'] or ""):

    if event['app_name'] is None:
        continue

    # 初回動作チェック
    if PREV_APP_ID == "":
        i = 0
    elif PREV_APP_ID == event['app_id']:
        i = 1
    else:
        PREV_APP_ID = event['app_id']
        i = 0
    PREV_APP_ID = event['app_id']

    # 初期処理
    if i == 0:
        # スプレッドシート名生成
        if event['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + event['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + event['app_name'] + "_Android／シミュレーション"
    
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
            target = worksheet.append_row(['', "{}".format(CHECK_DATE)], value_input_option='USER_ENTERED')
            target = worksheet.find(str(CHECK_DATE))

        # リセット&数字を入れておく
        try:
            worksheet.update_cell(target.row, 28, 0)
            sleep(5)
            worksheet.update_cell(target.row, 29, 0)
        except:
            sleep(5)
            worksheet.update_cell(target.row, 28, 0)
            sleep(1)
            worksheet.update_cell(target.row, 29, 0)

    sleep(5)

    # 接続確認
    try:
        worksheet.update_cell(target.row, 3, event['app_name'])
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
        worksheet.update_cell(target.row, 3, event['app_name'])
        print(type(e))
        print(e)

    # DAU
    if DEBUG:
        print(event['app_name'] + " : " + event['platform'] + " : " + str(event['daily_active_users'] or '0') + " : " + str(event['tracked_installs'] or '0'))

    DAU_COL=28
    NEW_COL=29
    try:
        worksheet.update_cell(target.row, DAU_COL, int(worksheet.cell(target.row, DAU_COL).value) + int(event['daily_active_users'] or "0"))
        sleep(5)
        worksheet.update_cell(target.row, NEW_COL, int(worksheet.cell(target.row, NEW_COL).value) + int(event['tracked_installs'] or "0"))
    except:
        sleep(5)
        worksheet.update_cell(target.row, DAU_COL, int(worksheet.cell(target.row, DAU_COL).value) + int(event['daily_active_users'] or "0"))
        sleep(1)
        worksheet.update_cell(target.row, NEW_COL, int(worksheet.cell(target.row, NEW_COL).value) + int(event['tracked_installs'] or "0"))

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt end\n")

