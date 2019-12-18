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
LOG='/tmp/superset.log'
DEBUG=False
CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=31)
TODAY=datetime.date.today()

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_retention_tt start : " + str(CHECK_DATE) + "\n")

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

FILE_ID='1SEOARn8fSx5I5Tosq8fZ_Sl2BKxDmhowzFhZBW8CW1I'

PREV_APP_ID=""
retention_cells=[]

apps_retention=get_dict_resultset("WITH retention AS (SELECT \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 0 THEN 1 ELSE 0 END) AS day0, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 1 THEN 1 ELSE 0 END) AS day1, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 2 THEN 1 ELSE 0 END) AS day2, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 3 THEN 1 ELSE 0 END) AS day3, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 4 THEN 1 ELSE 0 END) AS day4, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 5 THEN 1 ELSE 0 END) AS day5, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 6 THEN 1 ELSE 0 END) AS day6, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 7 THEN 1 ELSE 0 END) AS day7, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 8 THEN 1 ELSE 0 END) AS day8, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 9 THEN 1 ELSE 0 END) AS day9, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 10 THEN 1 ELSE 0 END) AS day10, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 11 THEN 1 ELSE 0 END) AS day11, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 12 THEN 1 ELSE 0 END) AS day12, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 13 THEN 1 ELSE 0 END) AS day13, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 14 THEN 1 ELSE 0 END) AS day14, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 15 THEN 1 ELSE 0 END) AS day15, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 16 THEN 1 ELSE 0 END) AS day16, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 17 THEN 1 ELSE 0 END) AS day17, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 18 THEN 1 ELSE 0 END) AS day18, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 19 THEN 1 ELSE 0 END) AS day19, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 20 THEN 1 ELSE 0 END) AS day20, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 21 THEN 1 ELSE 0 END) AS day21, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 22 THEN 1 ELSE 0 END) AS day22, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 23 THEN 1 ELSE 0 END) AS day23, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 24 THEN 1 ELSE 0 END) AS day24, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 25 THEN 1 ELSE 0 END) AS day25, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 26 THEN 1 ELSE 0 END) AS day26, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 27 THEN 1 ELSE 0 END) AS day27, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 28 THEN 1 ELSE 0 END) AS day28, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 29 THEN 1 ELSE 0 END) AS day29, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 30 THEN 1 ELSE 0 END) AS day30, \
        (CASE WHEN DATEDIFF(day, date(acquired_at), date(created_at)) = 31 THEN 1 ELSE 0 END) AS day31, \
        a.id AS app_id, a.name AS app_name, date(acquired_at) AS date, a.platform AS platform \
        FROM events e \
        LEFT JOIN apps a ON a.id = e.app_id WHERE event = 'open' AND acquired_at BETWEEN '{}' AND '{}' GROUP BY a.id, a.name, date(acquired_at), date(e.created_at), advertising_id, a.platform) \
        SELECT \
        SUM(day0) AS day0, SUM(day1) AS day1, SUM(day2) AS day2, SUM(day3) AS day3, SUM(day4) AS day4, SUM(day5) AS day5, SUM(day6) AS day6, SUM(day7) AS day7, SUM(day8) AS day8, SUM(day9) AS day9, \
        SUM(day10) AS day10, SUM(day11) AS day11, SUM(day12) AS day12, SUM(day13) AS day13, SUM(day14) AS day14, SUM(day15) AS day15, SUM(day16) AS day16, SUM(day17) AS day17, SUM(day18) AS day18, SUM(day19) AS day19, SUM(day20) AS day20, SUM(day21) AS day21, SUM(day22) AS day22, SUM(day23) AS day23, SUM(day24) AS day24, SUM(day25) AS day25, SUM(day26) AS day26, SUM(day27) AS day27, SUM(day28) AS day28, SUM(day29) AS day29, SUM(day30) AS day30, SUM(day31) AS day31, \
        app_id, app_name, date, platform FROM retention GROUP BY date, app_id, app_name, platform".format(str(CHECK_DATE), str(TODAY)))

for retention in sorted(sorted(sorted(apps_retention, reverse=True, key=lambda x:x['day0']), key=lambda x:x['date']), key=lambda x:x['app_id'] or ""):

    if retention['app_name'] is None:
        continue
 
    # 初回動作チェック
    if PREV_APP_ID == "":
        i = 0
    elif PREV_APP_ID == retention['app_id']:
        i = 1
    else:
        i = 0
        PREV_APP_ID = retention['app_id']
        CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=31)

        try:
            # 区画を一気に書き込み
            sleep(3)
            target_cells2 = worksheet.range(11, 3, 41, 35)
            for (target_cell, A) in zip(target_cells2, retention_cells):
                target_cell.value=A
            retention_cells=[]
            sleep(2)
            worksheet.update_cells(target_cells2, value_input_option='USER_ENTERED')
        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_retention_tt : API制限 データ書き込み失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 データ書き込み失敗 スキップ" + SPREADSHEET_NAME)

            continue


    PREV_APP_ID = retention['app_id']

    # 初期処理
    if i == 0:
        #読み込み業初期化
        # スプレッドシート名生成
        if retention['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + retention['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + retention['app_name'] + "_Android／シミュレーション"

        if DEBUG:
            print(str(CHECK_DATE) + ": check_retention_tt : " + SPREADSHEET_NAME)

        # Googleスプレッドシート無ければ作成
        try:
            sleep(3)
            worksheet = gc.open(SPREADSHEET_NAME).worksheet("残存日数計算")
            if DEBUG:
                print("ファイルオープン成功")

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_retention_tt : API制限 ファイルオープン失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 ファイルオープン失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_retention_tt : ファイル作成 " + SPREADSHEET_NAME + "\n")
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
            worksheet = gc.open(SPREADSHEET_NAME).worksheet("残存日数計算")

    if DEBUG:
        print(str(retention['date']) + " : " + "Day0 Retention " + str(retention['day0']) + \
                 " : Day1 Retention " + str(round(retention['day1']/retention['day0']*100, 2)) + \
                "% : Day2 Retention " + str(round(retention['day2']/retention['day0']*100, 2)) + \
                "% : Day3 Retention " + str(round(retention['day3']/retention['day0']*100, 2)) + "% : " + str(retention['platform']) + " : " + str(retention['app_name']))

    # 書き込みデータ作成
    if str(retention['date']) != str(CHECK_DATE):
        for j in range((retention['date']-CHECK_DATE).days):
            retention_cells.append(str(CHECK_DATE))
            retention_cells.append(str(0))
            for x in range(31):
                retention_cells.append("0.00%")
            CHECK_DATE=CHECK_DATE+datetime.timedelta(days=1)

    retention_cells.append(str(retention['date']))
    retention_cells.append(str(retention['day0']))
    retention_cells.append(str(retention['day1']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day2']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day3']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day4']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day5']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day6']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day7']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day8']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day9']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day10']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day11']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day12']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day13']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day14']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day15']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day16']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day17']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day18']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day19']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day20']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day21']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day22']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day23']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day24']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day25']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day26']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day27']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day28']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day29']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day30']/retention['day0']*100) + "%")
    retention_cells.append(str(retention['day31']/retention['day0']*100) + "%")

    CHECK_DATE=CHECK_DATE+datetime.timedelta(days=1)

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_retention_tt end\n")
