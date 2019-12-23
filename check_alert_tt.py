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

#pygsheetsはどう？

args = sys.argv

DATABASE_URL='postgresql://'+ args[1] + ':' + args[2] + '@'+ args[3] + ':5439/'+ args[4]
SLACK_URL=args[5]
KEYFILE_PATH=args[6]
DATE=int(args[7])
LOG='/tmp/superset.log'
DEBUG=False
CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=DATE)
PREV_CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=(DATE+2))
FILE_ID='1SEOARn8fSx5I5Tosq8fZ_Sl2BKxDmhowzFhZBW8CW1I'

print(str(CHECK_DATE) + " : " + str(PREV_CHECK_DATE))

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_alert_tt start : " + str(CHECK_DATE) + "\n")

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

http = httplib2.Http(timeout=7200)
http = credentials.authorize(http)
service = build('drive', 'v3', http=http)

AGGREGATE=[]
PREV_APP_ID=""
# Redshiftから収入取得

# DAUブッ込み
apps_events=get_dict_resultset("SELECT a.bundle_id AS bundle_id, a.id AS app_id, a.name AS app_name, rm.platform AS platform, Sum(daily_active_users) AS daily_active_users, Sum(tracked_installs) AS tracked_installs FROM (reporting_metrics rm LEFT OUTER JOIN apps a ON rm.app_id = a.id) WHERE date = '{}' GROUP BY rm.app_id, rm.platform, a.name, a.id, a.bundle_id".format(str(CHECK_DATE)))
for event in sorted(sorted(apps_events, key=lambda x:x['app_id'] or ''), key=lambda x:x['bundle_id'] or ''):
    if event['app_id'] is not None:
        AGGREGATE.append({'app_id': str(event['app_id']), 'bundle_id': str(event['bundle_id']), 'app_name': str(event['app_name']), 'platform': str(event['platform']), 'ad_name': '', 'date': str(CHECK_DATE), 'revenue': 0, 'spend': 0, 'dau': str(event['daily_active_users']), 'install': str(event['tracked_installs']), 'purchase': 0})

# グルグルして書き込みデータ作る
PREV_APP_ID=""
for aggregate in sorted(sorted(AGGREGATE, key=lambda x:x['app_id'] or ''), key=lambda x:x['bundle_id'] or ''):

    # 初回動作チェック
    if PREV_APP_ID == "":
        i = 0
        PREV_APP_ID = aggregate['app_id']

    elif PREV_APP_ID == aggregate['app_id']:
        i = 1
    else:
        i = 0
        PREV_APP_ID = aggregate['app_id']

    PREV_APP_ID = aggregate['app_id']

    # 初期処理
    if i == 0:
        # スプレッドシート名生成 新規ファイル作成時に必要
        if aggregate['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + aggregate['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + aggregate['app_name'] + "_Android／シミュレーション"

        if DEBUG:
            print(str(CHECK_DATE) + ": check_alert_tt : " + SPREADSHEET_NAME)

        # Googleスプレッドシートを開く無ければ作成
        try:
            # Tenjin の APP_ID からファイルを検索してIDゲット
            sleep(1)
            results = service.files().list(q="fullText contains '{}'".format(aggregate['app_id']), pageSize=1).execute()
            SPREADSHEET_KEY=results['files'][0]['id']

            if DEBUG:
                print("Tenjin APP_ID 検索結果 NAME: " + results['files'][0]['name'] + " ID: " + results['files'][0]['id'])

            print("Tenjin APP_ID 検索結果 NAME: " + results['files'][0]['name'] + " ID: " + results['files'][0]['id'])

            sleep(1)
            # 昔はファイル名で開いてました
#            worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
            #Tenjinのapp_id検索して開く
            workbook = gc.open_by_key(SPREADSHEET_KEY)
            worksheet = workbook.worksheet("売上集計")

            if DEBUG:
                print("ファイルオープン成功")

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_alert_tt : API制限 ファイルオープン失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 ファイルオープン失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            print(aggregate)
            print(type(e))
            print(str(e))

        # 当日行取得、無ければ作る
        try:
            sleep(1)
            prev_target = worksheet.find(str(PREV_CHECK_DATE))
            sleep(1)
            prev_target_cells = worksheet.range(prev_target.row, prev_target.col + 108, prev_target.row, prev_target.col + 108)
            prev_aldau=float(prev_target_cells[0].value.replace("¥", ""))
            print(str(prev_target_cells[0].value))

            sleep(1)
            target = worksheet.find(str(CHECK_DATE))
            sleep(1)
            target_cells = worksheet.range(target.row, target.col + 108, target.row, target.col + 108)
            aldau=float(target_cells[0].value.replace("¥", ""))
            print(str(target_cells[0].value))

            print("警告 : AdMob レクタングル DAU貢献度 2日前比 -10円を超えています : {} : {}円".format(SPREADSHEET_NAME, str(round(aldau-prev_aldau, 2))))

            if round(aldau-prev_aldau, 2) < -10.0:
                data = json.dumps({
                    'text': "警告 : AdMob レクタングル DAU貢献度 2日前比 -10円を超えています : {} : {}円".format(SPREADSHEET_NAME, str(round(aldau-prev_aldau, 2))),  #通知内容
                    'username': u'人格を喪失した林家パー子',  #ユーザー名
#                    'icon_emoji': u':smile_cat:',  #アイコン
                    'link_names': 1,  #名前をリンク化
                })
                requests.post("https://hooks.slack.com/services/" + SLACK_URL, data)

        # 無いなら諦める
        except gspread.exceptions.CellNotFound as e:
            if DEBUG:
                print(type(e))
                print("対象行無し")
                print(e)

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_alert_tt : API制限 当日行処理失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 当日行処理失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            print(type(e))

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_alert_tt end\n")

