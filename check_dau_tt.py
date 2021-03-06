#!/usr/bin/python3.7
# -*- coding: utf8 -*-

import requests
import os
import psycopg2
import psycopg2.extras
import datetime
import sys

import gspread
import json

from oauth2client.service_account import ServiceAccountCredentials

from googleapiclient.discovery import build
import httplib2

from time import sleep

args = sys.argv

DATABASE_URL='postgresql://'+ args[1] + ':' + args[2] + '@'+ args[3] + ':5439/'+ args[4]
SLACK_URL=args[5]
KEYFILE_PATH=args[6]
DATE=int(args[7])
LOG='/tmp/superset.log'
DEBUG=False
CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=DATE)
FILE_ID='1SEOARn8fSx5I5Tosq8fZ_Sl2BKxDmhowzFhZBW8CW1I'

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt start : " + str(CHECK_DATE) + "\n")

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
credentials = ServiceAccountCredentials.from_json_keyfile_name(KEYFILE_PATH, scope)
gc = gspread.authorize(credentials)

http = httplib2.Http()
http = credentials.authorize(http)
service = build('drive', 'v3', http=http)

DAU_COL=28
NEW_COL=29

# RedshiftからDAU取得
apps_events=get_dict_resultset("SELECT a.bundle_id AS bundle_id, a.id AS app_id, a.name AS app_name, rm.platform AS platform, Sum(daily_active_users) AS daily_active_users, Sum(tracked_installs) AS tracked_installs FROM (reporting_metrics rm LEFT OUTER JOIN apps a ON rm.app_id = a.id) WHERE date = '{}' GROUP BY rm.app_id, rm.platform, a.name, a.id, a.bundle_id".format(str(CHECK_DATE)))
for event in sorted(sorted(apps_events, key=lambda x:x['app_id'] or ""), key=lambda x:x['bundle_id'] or ""):

    if event['app_name'] is None:
        if DEBUG:
            print("event['app_name'] is None")
            print(event)
        continue

    if (event['daily_active_users'] == 0) and (event['tracked_installs'] == 0):
        if DEBUG:
            print("(event['daily_active_users'] == 0) AND (event['tracked_installs'] == 0)")
            print(event)
        continue

    # スプレッドシート名生成
    if event['platform'] != 'android':
        SPREADSHEET_NAME="BOT_" + event['app_name'] + "／シミュレーション"
    else:
        SPREADSHEET_NAME="BOT_" + event['app_name'] + "_Android／シミュレーション"

    if DEBUG:
        print(str(CHECK_DATE) + ": check_dau_tt : " + SPREADSHEET_NAME)

    # Googleスプレッドシート無ければ作成
    try:
        sleep(4)
        worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
        if DEBUG:
            print("ファイルオープン成功")

    except gspread.exceptions.APIError as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt : API制限 ファイルオープン失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print(type(e))
            print("API制限 ファイルオープン失敗 スキップ " + SPREADSHEET_NAME)
        continue

        # OAuth処理
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        sleep(5)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(KEYFILE_PATH, scope)
        sleep(5)
        gc = gspread.authorize(credentials)
        sleep(5)
        worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")

    except Exception as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt : ファイル作成 " + SPREADSHEET_NAME + "\n")
            f.write(str(type(e)) + "\n")
            f.write(str(e) + "\n")

        new_file_body = {
            'name': SPREADSHEET_NAME,  # 新しいファイルのファイル名. 省略も可能
            'parents': ['1Z6nHs-LoO8D_HdXuY2wkH5yd2Uh70daP'],  # Copy先のFolder ID. 省略も可能
        }

        if DEBUG:
            print("ファイル作成")
            print(FILE_ID)

        sleep(4)
        new_file = service.files().copy(
            fileId=FILE_ID, body=new_file_body
        ).execute()

        gc = gspread.authorize(credentials)
        sleep(4)
        worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")

    # 当日行取得、無ければ作る
    try:
        sleep(3)
        target = worksheet.find(str(CHECK_DATE))
        sleep(3)
        target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 44)
        target_cells[2].value=event['app_name']

    # 無いので行を追加
    except gspread.exceptions.CellNotFound as e:
        if DEBUG:
            print(type(e))
            print("新規追加")
            print(e)

        # 売上集計シートに行追加
        sleep(4)
        sales_summary = gc.open(SPREADSHEET_NAME).worksheet("売上集計")
        # リファレンス行をコピー
        sleep(4)
        reference_list = sales_summary.row_values(11, value_render_option='FORMULA')

        # 最終行にペースト
        del reference_list[0]
        sleep(4)
        last = sales_summary.append_row(reference_list, value_input_option='USER_ENTERED')

        # 関数修正
#        last_row=gspread.utils.a1_to_rowcol(last['updates']['updatedRange'].replace("'売上集計'!", ""))[0]
#        collection_cells = sales_summary.range(11, 108, last_row, 108)
#        for collection in collection_cells:
#            collection.value="=IFERROR(INDIRECT(ADDRESS(row(),107))*F$2)"
#        sales_summary.update_cells(collection_cells, value_input_option='USER_ENTERED')

        # 関数修正
        last_row=gspread.utils.a1_to_rowcol(last['updates']['updatedRange'].replace("'売上集計'!", ""))[0]
        collection_cells = sales_summary.range(11, 20, last_row, 20)
        for collection in collection_cells:
            collection.value="=INDIRECT(ADDRESS(row(),6))-SUM(INDIRECT(ADDRESS(row(),7)):INDIRECT(ADDRESS(row(),19)))"
        sales_summary.update_cells(collection_cells, value_input_option='USER_ENTERED')

        # 関数修正
#        fix_lines=['CPI IronSource', 'CPI SearchAds', 'CPI Mintegral', 'CPI TikTok', 'CPI UAC', 'CPI i-mobileAffiliate(CPI)', 'CPI UnityAds', 'CPI AppLovin', 'CPI TapjoyCPV', 'CPI Sanapchat']
#        collection_cells = sales_summary.range(10, 96, 10, 105)
#        for collection, fix in zip(collection_cells, fix_lines):
#            print(str(collection.value) + " : " + str(fix))
#            collection.value=fix
#        sales_summary.update_cells(collection_cells, value_input_option='USER_ENTERED')

        # 書き込みデータ作成
        target_list=['']*29
        target_list[1]=str(CHECK_DATE)
        target_list[2]=event['app_name']

        # 最終行に追加
        sleep(4)
        worksheet.append_row(target_list, value_input_option='USER_ENTERED')

        # 追加行取得
        sleep(4)
        target = worksheet.find(str(CHECK_DATE))
        sleep(4)
        target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 44)

    except gspread.exceptions.APIError as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt : API制限 当日行処理失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print(type(e))
            print("API制限 当日行処理失敗 スキップ " + SPREADSHEET_NAME)
        continue

    except Exception as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt : 新規エラー 当日行処理失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print(type(e))
            print("新規エラー")
        continue

    # データ書き込み
    try:
        # 書き込みデータ作成
        target_cells[27].value=event['daily_active_users']
        target_cells[28].value=event['tracked_installs']

#        sleep(1)
        worksheet.update_cells(target_cells, value_input_option='USER_ENTERED')

    except gspread.exceptions.APIError as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt : API制限 データ書き込み失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print(type(e))
            print("API制限 データ書き込み失敗 スキップ " + SPREADSHEET_NAME)
        continue

    except Exception as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt : 新規エラー データ書き込み失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print(type(e))
            print("新規エラー")
        continue

    # DAU
    if DEBUG:
        print(event['app_name'] + " : " + event['platform'] + " : " + str(event['daily_active_users'] or '0') + " : " + str(event['tracked_installs'] or '0'))

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_dau_tt end\n")

