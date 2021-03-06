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

print(str(args[1]))
DATABASE_URL='postgresql://'+ args[1] + ':' + args[2] + '@'+ args[3] + ':5439/'+ args[4]
SLACK_URL=args[5]
KEYFILE_PATH=args[6]
DATE=int(args[7])
LOG='/tmp/superset.log'
DEBUG=False
CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=DATE)
FILE_ID='1SEOARn8fSx5I5Tosq8fZ_Sl2BKxDmhowzFhZBW8CW1I'

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_tt start : " + str(CHECK_DATE) + "\n")

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
        i = 0
        PREV_APP_ID = revenue['app_id']

        try:
            sleep(1)
            worksheet.update_cells(target_cells, value_input_option='USER_ENTERED')

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_tt : API制限 データ書き込み失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 データ書き込み失敗 スキップ" + SPREADSHEET_NAME)
            continue

    PREV_APP_ID = revenue['app_id']

    # 初期処理
    if i == 0:
        # スプレッドシート名生成
        if revenue['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + revenue['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + revenue['app_name'] + "_Android／シミュレーション"

        if DEBUG:
            print(str(CHECK_DATE) + ": check_revenue_tt : " + SPREADSHEET_NAME)

        # Googleスプレッドシート無ければ作成
        try:
            sleep(4)
            worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
            if DEBUG:
                print("ファイルオープン成功")

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_tt : API制限 ファイルオープン失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 ファイルオープン失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_tt : ファイル作成 " + SPREADSHEET_NAME + "\n")
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
            target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 47)
            target_cells[2].value=revenue['app_name']

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
            target_list[2]=revenue['app_name']

            # 最終行に追加
            sleep(4)
            worksheet.append_row(target_list, value_input_option='USER_ENTERED')

            # 最終行に日付追加
            sleep(4)
            target = worksheet.find(str(CHECK_DATE))
            sleep(4)
            target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 47)

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_tt : API制限 当日行処理失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 当日行処理失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            if DEBUG:
                print(type(e))
                print("新規エラー")

        # ゼロ埋め初期化
        target_cells[5].value=0
        target_cells[9].value=0
        target_cells[10].value=0
        target_cells[11].value=0
        target_cells[12].value=0
        target_cells[14].value=0
        target_cells[15].value=0
        target_cells[16].value=0
        target_cells[29].value=0
        target_cells[34].value=0
        target_cells[35].value=0
        target_cells[47].value=0

    # revenueをドルに戻す
    REVENUE=revenue['revenue']/100
    if DEBUG:
        print(revenue['app_name'] + " : " + revenue['platform'] + " : " + revenue['ad_name'] + " : " + " : 広告収入 $" + str(REVENUE))

    # Unity Ads収入
    if revenue['ad_name'] == 'Unity Ads':
        target_cells[5].value=REVENUE

    # nend収入
#    if revenue['ad_name'] == '':
#        worksheet.update_cell(target.row, 9, REVENUE)

    # Ad Generation収入
    elif revenue['ad_name'] == 'Ad Generation':
        target_cells[9].value=REVENUE

    # FIVE収入
    elif revenue['ad_name'] == 'FIVE':
        target_cells[10].value=REVENUE

    # Maio収入
    elif revenue['ad_name'] == 'Maio':
        target_cells[11].value=REVENUE

    # i-mobile Affiliate収入
    elif revenue['ad_name'] == 'i-mobile Affiliate':
        target_cells[12].value=REVENUE

    # ironSource-Publisher収入
    elif revenue['ad_name'] == 'ironSource-Publisher':
        target_cells[14].value=REVENUE

    # Tapjoy収入
    elif revenue['ad_name'] == 'Tapjoy':
        target_cells[15].value=REVENUE

    # Facebook Audience Network収入
    elif revenue['ad_name'] == 'Facebook Audience Network':
        target_cells[16].value=REVENUE

    # Vungle収入
    elif revenue['ad_name'] == 'Vungle':
        target_cells[29].value=REVENUE

    # TikTok Audience Network収入
    elif revenue['ad_name'] == 'TikTok Audience Network':
        target_cells[34].value=REVENUE

    # Mintegral Publisher収入
    elif revenue['ad_name'] == 'Mintegral Publisher':
        target_cells[35].value=REVENUE

    # inmobi収入
    elif revenue['ad_name'] == 'inmobi':
        target_cells[47].value=REVENUE

    # Google AdMob収入
    elif revenue['ad_name'] == 'Google AdMob':
        if DEBUG:
            print(str(revenue['ad_name']) + " : " + str(REVENUE))

    # Applovin収入
    elif revenue['ad_name'] == 'Applovin':
        if DEBUG:
            print(str(revenue['ad_name']) + " : " + str(REVENUE))

    else:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_tt : 新規アドネットワーク : " + str(revenue['ad_name']) + " : " + str(REVENUE) +  " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print("DEBUG DEBUG DEBUG!")
            print(str(revenue['ad_name']) + " : " + str(REVENUE))

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_tt end\n")

