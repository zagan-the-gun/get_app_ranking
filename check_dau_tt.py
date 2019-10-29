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
DEBUG=False
CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=DATE)

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt start\n")

if DEBUG:
    print(CHECK_DATE)

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

DAU_COL=28
NEW_COL=29

# RedshiftからDAU取得
apps_events=get_dict_resultset("SELECT a.bundle_id AS bundle_id, a.id AS app_id, a.name AS app_name, rm.platform AS platform, Sum(daily_active_users) AS daily_active_users, Sum(tracked_installs) AS tracked_installs FROM (reporting_metrics rm LEFT OUTER JOIN apps a ON rm.app_id = a.id) WHERE date = '{}' GROUP BY rm.app_id, rm.platform, a.name, a.id, a.bundle_id".format(str(CHECK_DATE)))
for event in sorted(sorted(apps_events, key=lambda x:x['app_id'] or ""), key=lambda x:x['bundle_id'] or ""):

    if event['app_name'] is None:
        continue

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
#        target_list = worksheet.row_values(worksheet.find(str(CHECK_DATE)).row, value_render_option='FORMULA')
#        target_list = worksheet.row_values(worksheet.find(str(CHECK_DATE)).row, value_render_option='UNFORMATTED_VALUE')
        target_list = worksheet.row_values(worksheet.find(str(CHECK_DATE)).row, value_render_option='FORMATTED_VALUE')
        # 一度行を削除
        worksheet.delete_row(worksheet.find(str(CHECK_DATE)).row)

        # 書き込みデータ作成
#        target_list[1]=str(CHECK_DATE)
#        target_list[2]=event['app_name']
#        target_list[27]=event['daily_active_users']
#        target_list[28]=event['tracked_installs']

        # 最終行に追加
#        worksheet.append_row(target_list, value_input_option='USER_ENTERED')

    # 無いので行を追加
    except gspread.exceptions.CellNotFound as e:
        if DEBUG:
            print(type(e))
            print("新規追加")
            print(e)

        # 売上集計シートに行追加
        sales_summary = gc.open(SPREADSHEET_NAME).worksheet("売上集計")
        # リファレンス行をコピー
        reference_list = sales_summary.row_values(11, value_render_option='FORMULA')
        # 最終行にペースト
        del reference_list[0]
        sales_summary.append_row(reference_list, value_input_option='USER_ENTERED')

        # 書き込みデータ作成
        target_list=['']*29
#        target_list[1]=str(CHECK_DATE)
#        target_list[2]=event['app_name']
#        target_list[27]=event['daily_active_users']
#        target_list[28]=event['tracked_installs']

        # 最終行に追加
#        worksheet.append_row(target_list, value_input_option='USER_ENTERED')

    except gspread.exceptions.APIError as e:
        print(type(e))
        print("API制限 スキップ")
        continue

    except Exception as e:
        print(type(e))

    # データ書き込み
    try:
        # 書き込みデータ作成
        target_list[1]=str(CHECK_DATE)
        target_list[2]=event['app_name']
        target_list[27]=event['daily_active_users']
        target_list[28]=event['tracked_installs']

        # 最終行に追加
        worksheet.append_row(target_list, value_input_option='USER_ENTERED')

    except gspread.exceptions.APIError as e:
        print(type(e))
        print("API制限10秒待機")
        sleep(10)

    except Exception as e:
        print(type(e))


    # DAU
    if DEBUG:
        print(event['app_name'] + " : " + event['platform'] + " : " + str(event['daily_active_users'] or '0') + " : " + str(event['tracked_installs'] or '0'))
        print("END")

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt end\n")

