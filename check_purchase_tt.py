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

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_purchase_tt start : " + str(CHECK_DATE) + "\n")

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
#apps_purchase=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, store_id, platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, dar.date, Sum(revenue) AS revenue, Sum(impressions) AS impressions, Sum(clicks) AS clicks, Sum(conversions) AS conversions FROM (((daily_ad_revenue dar LEFT OUTER JOIN publisher_apps pa ON dar.publisher_app_id = pa.id) LEFT OUTER JOIN apps a ON pa.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON pa.ad_network_id = ad.id) WHERE date = '{}' GROUP BY pa.ad_network_id, a.id, a.name, store_id, platform, bundle_id, ad.id, ad.name, dar.date".format(str(CHECK_DATE)))

"""
print("daily_behavior テーブル")
{'date': datetime.datetime(2019, 12, 1, 0, 0),
 'campaign_id': '11456464-3270-49fa-b8d2-467553543f93',
 'country': 'CN',
 'site_id': '187079',
 'users': 2,
 'weekly_users': None,
 'monthly_users': None,
 'sessions': 2,
 'revenue': 390,
 'transactions': None}
apps_purchase=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, a.platform AS platform, SUM(revenue) AS revenue FROM ((daily_behavior db LEFT OUTER JOIN campaigns c ON db.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) WHERE revenue != 0 AND date = '{}' GROUP BY a.id, a.name, a.platform".format(str(CHECK_DATE)))
for purchase in sorted(sorted(apps_purchase, key=lambda x:x['app_id'] or ""), key=lambda x:x['platform'] or ""):
#    print(str(purchase))
    print("{} : {} : ${}".format(str(purchase['app_name'] or "None"), str(purchase['platform'] or "None"), str(purchase['revenue']/100)))
print("")
"""

"""
print("cohort_behavior テーブル")
{'date': datetime.date(2019, 12, 1),
 'xday': 0,
 'campaign_id': '9e86c219-d52e-435e-87c6-414275268602',
 'country': 'JP',
 'site_id': 'mobc9926d3cdffa3640',
 'users': 52,
 'sessions': 96,
 'revenue': 237,
 'transactions': None}
apps_purchase=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, a.platform AS platform, SUM(revenue) AS revenue FROM ((cohort_behavior cb LEFT OUTER JOIN campaigns c ON cb.campaign_id = c.id) LEFT OUTER JOIN apps a ON c.app_id = a.id) WHERE revenue != 0 AND date = '{}' GROUP BY a.id, a.name, a.platform".format(str(CHECK_DATE)))
for purchase in sorted(sorted(apps_purchase, key=lambda x:x['app_id'] or ""), key=lambda x:x['platform'] or ""):
#    print(str(purchase))
    print("{} : {} : ${}".format(str(purchase['app_name'] or "None"), str(purchase['platform'] or "None"), str(purchase['revenue']/100)))
print("")
"""

print("reporting_cohort_metrics テーブル")
"""
{'event_date': datetime.date(2019, 12, 1),
 'install_date': datetime.date(2019, 12, 1),
 'days_since_install': 0,
 'ad_network_id': 19215,
 'platform': 'ios',
 'app_id': 'f8918e18-7a46-47db-95b9-3980590d0879',
 'campaign_id': '56a0230c-3e21-40a6-981a-38cf57afd8e5',
 'country': 'JP',
 'site_id': 'p1548140_3_ddjhvumm',
 'daily_active_users': 3,
 'sessions': 23,
 'iap_revenue': 237,
 'publisher_ad_revenue': 311.219776503918}
"""
apps_purchase=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, a.platform AS platform, SUM(iap_revenue) AS revenue FROM (reporting_cohort_metrics rcm LEFT OUTER JOIN apps a ON rcm.app_id = a.id) WHERE iap_revenue != 0 AND event_date = '{}' GROUP BY a.id, a.name, a.platform".format(str(CHECK_DATE)))
"""
for purchase in sorted(sorted(apps_purchase, key=lambda x:x['app_id'] or ""), key=lambda x:x['platform'] or ""):
#    print(str(purchase))
    print("{} : {} : ${}".format(str(purchase['app_name'] or "None"), str(purchase['platform'] or "None"), str(purchase['revenue']/100)))
print("")
"""

"""
print("reporting_metrics テーブル")
{'date': datetime.date(2019, 12, 1),
 'ad_network_id': 29,
 'platform': 'ios',
 'app_id': 'f3c1becb-b9bc-47a8-a6dd-476bb1eafc84',
 'campaign_id': '1c9e3ded-aee8-4bfa-ad60-d0cc645127ee',
 'country': 'JP',
 'site_id': '266773',
 'daily_active_users': 184,
 'sessions': 527,
 'iap_revenue': 237,
 'reported_spend': 7893.99696048632,
 'reported_impressions': 6064.3905775076,
 'reported_clicks': 1886.81534954407,
 'reported_installs': 78.9399696048632,
 'publisher_ad_revenue': 8456.25052380113,
 'tracked_installs': 79,
 'tracked_clicks': 2165,
 'tracked_impressions': 8059}
apps_purchase=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, a.platform AS platform, SUM(iap_revenue) AS revenue FROM (reporting_metrics rm LEFT OUTER JOIN apps a ON rm.app_id = a.id) WHERE iap_revenue != 0 AND date = '{}' GROUP BY a.id, a.name, a.platform".format(str(CHECK_DATE)))
for purchase in sorted(sorted(apps_purchase, key=lambda x:x['app_id'] or ""), key=lambda x:x['platform'] or ""):
#    print(str(purchase))
    print("{} : {} : ${}".format(str(purchase['app_name'] or "None"), str(purchase['platform'] or "None"), str(purchase['revenue']/100)))
print("")
"""

for purchase in reversed(sorted(sorted(apps_purchase, key=lambda x:x['app_id'] or ""), key=lambda x:x['platform'] or "")):

    if purchase['app_name'] is None:
        if DEBUG:
            print("purchase['app_name'] is None")
            print(purchase)
        continue

    # スプレッドシート名生成
    if purchase['platform'] != 'android':
        SPREADSHEET_NAME="BOT_" + purchase['app_name'] + "／シミュレーション"
    else:
        SPREADSHEET_NAME="BOT_" + purchase['app_name'] + "_Android／シミュレーション"

    if DEBUG:
        print(str(CHECK_DATE) + ": check_purchase_tt : " + SPREADSHEET_NAME)

    # Googleスプレッドシート無ければ作成
    try:
        sleep(4)
        worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
        if DEBUG:
            print("ファイルオープン成功")

    except gspread.exceptions.APIError as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_purchase_tt : API制限 ファイルオープン失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print(type(e))
            print("API制限 ファイルオープン失敗 スキップ " + SPREADSHEET_NAME)
        continue

    except Exception as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_purchase_tt : ファイル作成 " + SPREADSHEET_NAME + "\n")
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
        target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 46)
        target_cells[2].value=purchase['app_name']

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
        target_list[2]=purchase['app_name']

        # 最終行に追加
        sleep(4)
        worksheet.append_row(target_list, value_input_option='USER_ENTERED')

        # 最終行に日付追加
        sleep(4)
        target = worksheet.find(str(CHECK_DATE))
        sleep(4)
        target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 46)

    except gspread.exceptions.APIError as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_purchase_tt : API制限 当日行処理失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print(type(e))
            print("API制限 当日行処理失敗 スキップ " + SPREADSHEET_NAME)
        continue

    except Exception as e:
        if DEBUG:
            print(type(e))
            print("新規エラー")

    if DEBUG:
        print("{} : {} : 課金収入 ${}".format(purchase['app_name'], purchase['platform'], str(purchase['revenue']/100)))

    # データ書き込み
    try:
        # 書き込みデータ作成
        target_cells[46].value=purchase['revenue']/100

        worksheet.update_cells(target_cells, value_input_option='USER_ENTERED')

    except gspread.exceptions.APIError as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_purchase_tt : API制限 データ書き込み失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print(type(e))
            print("API制限 データ書き込み失敗 スキップ " + SPREADSHEET_NAME)
        continue

    except Exception as e:
        with open(LOG, mode='a') as f:
            f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_purchase_tt : 新規エラー データ書き込み失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

        if DEBUG:
            print(type(e))
            print("新規エラー")
        continue

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_purchase_tt end\n")

