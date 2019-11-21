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
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_detail_tt start : " + str(CHECK_DATE) + "\n")

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

FILE_ID='1ogWdNTKbLVfGGD7-EMrBWfbwXzu_swn7fqmQk2X-2Us'

PREV_APP_ID=""
# Redshiftから収入取得
apps_pa_revenue=get_dict_resultset("SELECT Sum(offerwall_revenue) AS offerwall_revenue, Sum(banner_revenue) AS banner_revenue, Sum(interstitial_revenue) AS interstitial_revenue, Sum(native_revenue) AS native_revenue, Sum(video_revenue) AS video_revenue, Sum(other_revenue) AS other_revenue, pa.name AS pa_name, a.id AS app_id, a.name AS app_name, platform, bundle_id, ad.name AS ad_name, Sum(revenue) AS revenue FROM (((daily_ad_revenue dar LEFT OUTER JOIN publisher_apps pa ON dar.publisher_app_id = pa.id) LEFT OUTER JOIN apps a ON pa.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON pa.ad_network_id = ad.id) WHERE date = '{}' GROUP BY pa.ad_network_id, pa.name, a.id, a.name, store_id, platform, bundle_id, ad.name".format(str(CHECK_DATE)))
for pa_revenue in sorted(sorted(apps_pa_revenue, key=lambda x:x['app_id']), key=lambda x:x['bundle_id']):

    # 初回動作チェック
    if PREV_APP_ID == "":
        i = 0
    elif PREV_APP_ID == pa_revenue['app_id']:
        i = 1
    else:
        i = 0
        PREV_APP_ID = pa_revenue['app_id']

        try:
            sleep(1)
            worksheet.update_cells(target_cells, value_input_option='USER_ENTERED')

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_detail_tt : API制限 データ書き込み失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")
            if DEBUG:
                print(type(e))
                print("API制限 データ書き込み失敗 スキップ " + SPREADSHEET_NAME)
            continue

    PREV_APP_ID = pa_revenue['app_id']

    # 初期処理
    if i == 0:
        # スプレッドシート名生成
        if pa_revenue['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + pa_revenue['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + pa_revenue['app_name'] + "_Android／シミュレーション"
    
        if DEBUG:
            print(str(CHECK_DATE) + ": check_revenue_detail_tt : " + SPREADSHEET_NAME)
    
        # Googleスプレッドシート無ければ作成
        try:
            sleep(3)
            worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
            if DEBUG:
                print("ファイルオープン成功")

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_detail_tt : API制限 ファイルオープン失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 ファイルオープン失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_detail_tt : ファイル作成 " + SPREADSHEET_NAME + "\n")
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
            sleep(4)
            target = worksheet.find(str(CHECK_DATE))
            sleep(4)
            target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 44)

            # 初期化
            target_cells[2].value=pa_revenue['app_name']
            target_cells[3].value=0
            target_cells[4].value=0
            target_cells[6].value=0
            target_cells[7].value=0

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
            target_list=['']*8
            target_list[1]=str(CHECK_DATE)
            target_list[2]=pa_revenue['app_name']
            target_list[3]=0
            target_list[4]=0
            target_list[6]=0
            target_list[7]=0

            # 最終行に追加
            sleep(4)
            worksheet.append_row(target_list, value_input_option='USER_ENTERED')

            # 取得
            sleep(4)
            target = worksheet.find(str(CHECK_DATE))
            sleep(4)
            target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 44)

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_detail_tt : API制限 当日行処理失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")
            if DEBUG:
                print(type(e))
                print("API制限 当日行処理失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            if DEBUG:
                print(type(e))
                print("新規エラー")

    # revenueをドルに戻す
    REVENUE=pa_revenue['revenue']/100

    # Google AdMob出稿
    if 'Google AdMob' in pa_revenue['ad_name']:
        if DEBUG:
            print(pa_revenue['app_name'] + " : " + pa_revenue['pa_name'] + " : " + pa_revenue['platform'] + " : " + str(pa_revenue['bundle_id']) + " : " + pa_revenue['ad_name'] + " : 広告収入 $" + str(REVENUE))
        COL_BANNER=3
        COL_INTERSTITIAL=4
        if 'バナー' in pa_revenue['pa_name'] or 'Banner' in pa_revenue['pa_name'] or 'banner' in pa_revenue['pa_name'] or 'レクタングル' in pa_revenue['pa_name'] or 'Rectangle' in pa_revenue['pa_name'] or 'rectangle' in pa_revenue['pa_name']:
            target_cells[COL_BANNER].value=float(target_cells[COL_BANNER].value or 0) + float(REVENUE)

        else:
            target_cells[COL_INTERSTITIAL].value=float(target_cells[COL_INTERSTITIAL].value or 0) + float(REVENUE)

    # Applovin出稿
    elif 'Applovin' in pa_revenue['ad_name']:
        if DEBUG:
            print(pa_revenue['app_name'] + " : " + pa_revenue['pa_name'] + " : " + pa_revenue['platform'] + " : " + str(pa_revenue['bundle_id']) + " : " + pa_revenue['ad_name'] + " : 広告収入 $" + str(REVENUE))

        COL_BANNER=6
        COL_INTERSTITIAL=7
        # Applovinはカラムで振り分け
        target_cells[COL_BANNER].value=float(target_cells[COL_BANNER].value or 0) + (float(pa_revenue['offerwall_revenue'])/100) + (float(pa_revenue['banner_revenue'])/100) + (float(pa_revenue['native_revenue'])/100)
        target_cells[COL_INTERSTITIAL].value=float(target_cells[COL_INTERSTITIAL].value or 0) + (float(pa_revenue['interstitial_revenue'])/100) + (float(pa_revenue['video_revenue'])/100) + (float(pa_revenue['other_revenue'])/100)

    else:
        if DEBUG:
            print(pa_revenue['app_name'] + " : " + pa_revenue['pa_name'] + " : " + pa_revenue['platform'] + " : " + str(pa_revenue['bundle_id']) + " : " + pa_revenue['ad_name'] + " : 広告収入 $" + str(REVENUE))

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_revenue_detail_tt end\n")

