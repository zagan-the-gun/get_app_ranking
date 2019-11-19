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

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_spend_tt start\n")

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

FILE_ID='1ogWdNTKbLVfGGD7-EMrBWfbwXzu_swn7fqmQk2X-2Us'

print(str(CHECK_DATE))
PREV_APP_ID=""
# Redshiftから支出取得
#apps_spend=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, store_id, platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, ds.date, Sum(spend) AS spend, Sum(installs) AS installs, Sum(clicks) AS clicks, Sum(impressions) AS impressions, original_currency, Sum(original_spend) AS original_spend FROM (((daily_spend ds LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON c.ad_network_id = ad.id) WHERE date = '{}' GROUP BY c.ad_network_id, a.id, a.name, store_id, platform, bundle_id, ad.id, ad.name, ds.date, original_currency".format(str(CHECK_DATE)))

"""
apps_spend=get_dict_resultset("\
        SELECT a.id AS app_id, a.name AS app_name, store_id, a.platform AS platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, ds.date, Sum(spend) AS spend, Sum(installs) AS installs, Sum(clicks) AS clicks, Sum(impressions) AS impressions, original_currency, Sum(original_spend) AS original_spend \
        FROM (((daily_spend ds \
        LEFT OUTER JOIN campaigns c ON ds.campaign_id = c.id) \
        LEFT OUTER JOIN apps a ON c.app_id = a.id) \
        LEFT OUTER JOIN ad_networks ad ON c.ad_network_id = ad.id) \
        WHERE ds.date = '{}' \
        GROUP BY c.ad_network_id, a.id, a.name, store_id, a.platform, bundle_id, ad.id, ad.name, ds.date, original_currency".format(str(CHECK_DATE)))
"""

apps_spend=get_dict_resultset("\
        SELECT a.id AS app_id, a.name AS app_name, store_id, a.platform AS platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, rm.date, Sum(reported_spend) AS spend, Sum(tracked_installs) AS installs \
        FROM (((reporting_metrics rm \
        LEFT OUTER JOIN campaigns c ON rm.campaign_id = c.id) \
        LEFT OUTER JOIN apps a ON c.app_id = a.id) \
        LEFT OUTER JOIN ad_networks ad ON c.ad_network_id = ad.id) \
        WHERE rm.date = '{}' \
        GROUP BY c.ad_network_id, a.id, a.name, store_id, a.platform, bundle_id, ad.id, ad.name, rm.date".format(str(CHECK_DATE)))

for spend in sorted(sorted(apps_spend, key=lambda x:x['app_id'] or ""), key=lambda x:x['bundle_id'] or ""):

    if (spend['app_id'] is None) or (spend['ad_id'] is None):
        continue

    # 初回動作チェック
    if PREV_APP_ID == "":
        i = 0
    elif PREV_APP_ID == spend['app_id']:
        i = 1
    else:
        i = 0
        PREV_APP_ID = spend['app_id']

        try:
            sleep(2)
            worksheet.update_cells(target_cells, value_input_option='USER_ENTERED')

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_spend_tt : API制限 データ書き込み失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

    PREV_APP_ID = spend['app_id']

    # 初期処理
    if i == 0:
        # スプレッドシート名生成
        if spend['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + spend['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + spend['app_name'] + "_Android／シミュレーション"

        if DEBUG:
            print(str(CHECK_DATE) + ": check_spend_tt : " + SPREADSHEET_NAME)

        # Googleスプレッドシート無ければ作成
        try:
            sleep(4)
            worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
            if DEBUG:
                print("ファイルオープン成功")

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_spend_tt : API制限 ファイルオープン失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 ファイルオープン失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_spend_tt : ファイル作成 " + SPREADSHEET_NAME + "\n")

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
            sleep(4)
            target = worksheet.find(str(CHECK_DATE))
            sleep(4)
            target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 44)
            target_cells[2].value=spend['app_name']

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
            sales_summary.append_row(reference_list, value_input_option='USER_ENTERED')

            # 書き込みデータ作成
            target_list=['']*3
            target_list[1]=str(CHECK_DATE)
            target_list[2]=spend['app_name']

            # 最終行に追加
            sleep(4)
            worksheet.append_row(target_list, value_input_option='USER_ENTERED')

            # 最終行に日付追加
            sleep(4)
            target = worksheet.find(str(CHECK_DATE))

            sleep(4)
            target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 44)

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_spend_tt : API制限 当日行処理失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 当日行処理失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            print(type(e))

    # spendをドルに戻す
#    SPEND=spend['spend']/100
    SPEND=round((spend['spend'] or 0)/100, 2)
    if DEBUG:
        print(spend['app_name'] + " : " + spend['platform'] + " : " + str(spend['store_id']) + " : " + str(spend['bundle_id']) + " : " + spend['ad_name'] + " : インストール " + str(spend['installs'] or 0) + " : 広告支出 $" + str(SPEND))

    # Applovin出稿
    if spend['ad_name'] == 'Applovin':
        target_cells[17].value=spend['installs']
        target_cells[18].value=SPEND

    # Tapjoy出稿
    elif spend['ad_name'] == 'Tapjoy':
        target_cells[19].value=spend['installs']
        target_cells[20].value=SPEND

    # Unity出稿
    elif spend['ad_name'] == 'Unity Ads':
        target_cells[21].value=spend['installs']
        target_cells[22].value=SPEND

    # Facebook出稿
    elif spend['ad_name'] == 'Facebook':
        target_cells[23].value=spend['installs']
        target_cells[24].value=SPEND

    # ironSource出稿
    elif spend['ad_name'] == 'ironSource':
        target_cells[25].value=spend['installs']
        target_cells[26].value=SPEND

    # Google Ads出稿
    elif spend['ad_name'] == 'Google Ads':
        target_cells[30].value=spend['installs']
        target_cells[31].value=SPEND
        print(spend['ad_name'])
        print(spend)

    # TikTok出稿
    elif spend['ad_name'] == 'TikTok':
        target_cells[32].value=spend['installs']
        target_cells[33].value=SPEND

    # Toutiao出稿
    elif spend['ad_name'] == 'Toutiao':
        target_cells[32].value=spend['installs']
        target_cells[33].value=SPEND

    # Snapchat出稿
    elif spend['ad_name'] == 'Snapchat':
        target_cells[36].value=spend['installs']
        target_cells[37].value=SPEND

    # Mintegral出稿
    elif spend['ad_name'] == 'Mintegral':
        target_cells[38].value=spend['installs']
        target_cells[39].value=SPEND

    # Apple Search Ads出稿
    elif spend['ad_name'] == 'Apple Search Ads':
        target_cells[40].value=spend['installs']
        target_cells[41].value=SPEND

    # Maio出稿
    elif spend['ad_name'] == 'Maio':
        target_cells[42].value=spend['installs']
        target_cells[43].value=SPEND

    # i-mobile Affiliate出稿
    elif spend['ad_name'] == 'i-mobile Affiliate':
        target_cells[44].value=spend['installs']
        target_cells[45].value=SPEND

    else:
        print("DEBUG DEBUG DEBUG!")
        print(str(spend['ad_name']) + " : " + str(SPEND))

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_spend_tt end\n")

