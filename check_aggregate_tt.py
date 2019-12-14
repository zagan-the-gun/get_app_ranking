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

print(str(args[1]))
DATABASE_URL='postgresql://'+ args[1] + ':' + args[2] + '@'+ args[3] + ':5439/'+ args[4]
SLACK_URL=args[5]
KEYFILE_PATH=args[6]
DATE=int(args[7])
LOG='/tmp/superset.log'
DEBUG=True
CHECK_DATE=(datetime.date.today())-datetime.timedelta(days=DATE)
FILE_ID='1SEOARn8fSx5I5Tosq8fZ_Sl2BKxDmhowzFhZBW8CW1I'

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_aggregate_tt start : " + str(CHECK_DATE) + "\n")

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

# revenueブッ込み
apps_revenue=get_dict_resultset("SELECT a.id AS app_id, a.name AS app_name, store_id, platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, dar.date AS date, Sum(revenue) AS revenue, Sum(impressions) AS impressions, Sum(clicks) AS clicks, Sum(conversions) AS conversions FROM (((daily_ad_revenue dar LEFT OUTER JOIN publisher_apps pa ON dar.publisher_app_id = pa.id) LEFT OUTER JOIN apps a ON pa.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON pa.ad_network_id = ad.id) WHERE date = '{}' GROUP BY pa.ad_network_id, a.id, a.name, store_id, platform, bundle_id, ad.id, ad.name, dar.date".format(str(CHECK_DATE)))
for revenue in sorted(sorted(apps_revenue, key=lambda x:x['app_id'] or ''), key=lambda x:x['bundle_id'] or ''):
    if revenue['app_id'] is not None:
        AGGREGATE.append({'app_id': str(revenue['app_id']), 'bundle_id': str(revenue['bundle_id']), 'app_name': str(revenue['app_name']), 'platform': str(revenue['platform']), 'ad_name': str(revenue['ad_name']), 'date': str(revenue['date']), 'revenue': (revenue['revenue'] or 0)/100, 'spend': 0, 'dau': 0, 'install': 0, 'purchase': 0})



# revenue_detailは内部にもう一個fhr文つける対応が良さそう
apps_revenue_detail=get_dict_resultset("SELECT Sum(offerwall_revenue) AS offerwall_revenue, Sum(banner_revenue) AS banner_revenue, Sum(interstitial_revenue) AS interstitial_revenue, Sum(native_revenue) AS native_revenue, Sum(video_revenue) AS video_revenue, Sum(other_revenue) AS other_revenue, pa.name AS pa_name, a.id AS app_id, a.name AS app_name, platform, bundle_id, ad.name AS ad_name, Sum(revenue) AS revenue, dar.date AS date FROM (((daily_ad_revenue dar LEFT OUTER JOIN publisher_apps pa ON dar.publisher_app_id = pa.id) LEFT OUTER JOIN apps a ON pa.app_id = a.id) LEFT OUTER JOIN ad_networks ad ON pa.ad_network_id = ad.id) WHERE date = '{}' GROUP BY pa.ad_network_id, pa.name, a.id, a.name, store_id, platform, bundle_id, ad.name, dar.date".format(str(CHECK_DATE)))
PREV_APP_ID=""
ga_interstitial=0
ga_banner=0
al_interstitial=0
al_banner=0
for revenue_detail in sorted(sorted(apps_revenue_detail, key=lambda x:x['app_id'] or ''), key=lambda x:x['bundle_id'] or ''):
    #切り替わりチェック 切り替わった後に変更しようとしている
    if (PREV_APP_ID != revenue_detail['app_id']) and (PREV_APP_ID != ""):
        AGGREGATE.append({'app_id': str(APP_ID), 'bundle_id': str(BUNDLE_ID), 'app_name': str(APP_NAME), 'platform': str(PLATFORM), 'ad_name': 'Google AdMob interstitial', 'date': str(DATE), 'revenue': ga_interstitial/100, 'spend': 0, 'dau': '', 'install': '', 'purchase': 0})
        AGGREGATE.append({'app_id': str(APP_ID), 'bundle_id': str(BUNDLE_ID), 'app_name': str(APP_NAME), 'platform': str(PLATFORM), 'ad_name': 'Google AdMob banner', 'date': str(DATE), 'revenue': ga_banner/100, 'spend': 0, 'dau': '', 'install': '', 'purchase': 0})
        AGGREGATE.append({'app_id': str(APP_ID), 'bundle_id': str(BUNDLE_ID), 'app_name': str(APP_NAME), 'platform': str(PLATFORM), 'ad_name': 'Applovin interstitial', 'date': str(DATE), 'revenue': al_interstitial/100, 'spend': 0, 'dau': '', 'install': '', 'purchase': 0})
        AGGREGATE.append({'app_id': str(APP_ID), 'bundle_id': str(BUNDLE_ID), 'app_name': str(APP_NAME), 'platform': str(PLATFORM), 'ad_name': 'Applovin banner', 'date': str(DATE), 'revenue': al_banner/100, 'spend': 0, 'dau': '', 'install': '', 'purchase': 0})
        APP_ID=''
        APP_NAME=''
        BUNDLE_ID=''
        PLATFORM=''
        DATE=''
        ga_interstitial=0
        ga_banner=0
        al_interstitial=0
        al_banner=0

    # 力技だよなぁ・・・
    PREV_APP_ID=revenue_detail['app_id']
    APP_ID=revenue_detail['app_id']
    APP_NAME=revenue_detail['app_name']
    BUNDLE_ID=revenue_detail['bundle_id']
    PLATFORM=revenue_detail['platform']
    DATE=revenue_detail['date']

    # Google AdMob出稿
    if 'Google AdMob' in revenue_detail['ad_name']:
        if 'バナー' in revenue_detail['pa_name'] or 'Banner' in revenue_detail['pa_name'] or 'banner' in revenue_detail['pa_name'] or 'レクタングル' in revenue_detail['pa_name'] or 'Rectangle' in revenue_detail['pa_name'] or 'rectangle' in revenue_detail['pa_name']:
            ga_banner=(ga_banner or 0) + float(revenue_detail['revenue'] or 0)

        else:
            ga_interstitial=(ga_interstitial or 0) + float(revenue_detail['revenue'] or 0)

    # Applovin出稿
    elif 'Applovin' in revenue_detail['ad_name']:
        # Applovinはカラムで振り分け
        al_banner=(al_banner or 0) + float(revenue_detail['offerwall_revenue']) + float(revenue_detail['banner_revenue']) + float(revenue_detail['native_revenue'])

        al_interstitial=(al_interstitial or 0) + float(revenue_detail['interstitial_revenue']) + float(revenue_detail['video_revenue']) + float(revenue_detail['other_revenue'])

# spendブッ込み
apps_spend=get_dict_resultset("\
        SELECT a.id AS app_id, a.name AS app_name, store_id, a.platform AS platform, bundle_id, ad.id AS ad_id, ad.name AS ad_name, rm.date, Sum(reported_spend) AS spend, Sum(tracked_installs) AS installs \
        FROM (((reporting_metrics rm \
        LEFT OUTER JOIN campaigns c ON rm.campaign_id = c.id) \
        LEFT OUTER JOIN apps a ON c.app_id = a.id) \
        LEFT OUTER JOIN ad_networks ad ON c.ad_network_id = ad.id) \
        WHERE rm.date = '{}' \
        GROUP BY c.ad_network_id, a.id, a.name, store_id, a.platform, bundle_id, ad.id, ad.name, rm.date".format(str(CHECK_DATE)))
for spend in sorted(sorted(apps_spend, key=lambda x:x['app_id'] or ''), key=lambda x:x['bundle_id'] or ''):
    if spend['app_id'] is not None:
        AGGREGATE.append({'app_id': str(spend['app_id']), 'bundle_id': str(spend['bundle_id']), 'app_name': str(spend['app_name']), 'platform': str(spend['platform']), 'ad_name': str(spend['ad_name']), 'date': str(spend['date']), 'revenue': 0, 'spend': round((spend['spend'] or 0)/100, 2), 'dau': '', 'install': spend['installs'], 'purchase': 0})

# DAUブッ込み
apps_events=get_dict_resultset("SELECT a.bundle_id AS bundle_id, a.id AS app_id, a.name AS app_name, rm.platform AS platform, Sum(daily_active_users) AS daily_active_users, Sum(tracked_installs) AS tracked_installs FROM (reporting_metrics rm LEFT OUTER JOIN apps a ON rm.app_id = a.id) WHERE date = '{}' GROUP BY rm.app_id, rm.platform, a.name, a.id, a.bundle_id".format(str(CHECK_DATE)))
for event in sorted(sorted(apps_events, key=lambda x:x['app_id'] or ''), key=lambda x:x['bundle_id'] or ''):
    if event['app_id'] is not None:
        AGGREGATE.append({'app_id': str(event['app_id']), 'bundle_id': str(event['bundle_id']), 'app_name': str(event['app_name']), 'platform': str(event['platform']), 'ad_name': '', 'date': str(CHECK_DATE), 'revenue': 0, 'spend': 0, 'dau': str(event['daily_active_users']), 'install': str(event['tracked_installs']), 'purchase': 0})


# purchaseブッ込み
apps_purchase=get_dict_resultset("SELECT a.id AS app_id, a.bundle_id AS bundle_id, a.name AS app_name, a.platform AS platform, SUM(iap_revenue) AS revenue FROM (reporting_cohort_metrics rcm LEFT OUTER JOIN apps a ON rcm.app_id = a.id) WHERE iap_revenue != 0 AND event_date = '{}' GROUP BY a.id, a.bundle_id, a.name, a.platform".format(str(CHECK_DATE)))
for purchase in sorted(sorted(apps_purchase, key=lambda x:x['app_id'] or ''), key=lambda x:x['bundle_id'] or ''):
    if purchase['app_id'] is not None:
        AGGREGATE.append({'app_id': str(purchase['app_id']), 'bundle_id': str(purchase['bundle_id']), 'app_name': str(purchase['app_name']), 'platform': str(purchase['platform']), 'ad_name': str(spend['ad_name']), 'date': str(CHECK_DATE), 'revenue': 0, 'spend': 0, 'dau': '', 'install': '', 'purchase': (purchase['revenue'] or 0)/100})


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

        try:
            worksheet.update_cells(target_cells, value_input_option='USER_ENTERED')

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_aggregate_tt : API制限 データ書き込み失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 データ書き込み失敗 スキップ" + SPREADSHEET_NAME)

            continue

    PREV_APP_ID = aggregate['app_id']

    # 初期処理
    if i == 0:
        # スプレッドシート名生成 新規ファイル作成時に必要
        if aggregate['platform'] != 'android':
            SPREADSHEET_NAME="BOT_" + aggregate['app_name'] + "／シミュレーション"
        else:
            SPREADSHEET_NAME="BOT_" + aggregate['app_name'] + "_Android／シミュレーション"


        if DEBUG:
            print(str(CHECK_DATE) + ": check_aggregate_tt : " + SPREADSHEET_NAME)

        # Googleスプレッドシートを開く無ければ作成
        try:
            # Tenjin の APP_ID からファイルを検索してIDゲット
            sleep(1)
            results = service.files().list(q="fullText contains '{}'".format(aggregate['app_id']), pageSize=1).execute()
            SPREADSHEET_KEY=results['files'][0]['id']

            print("Tenjin APP_ID 検索結果 NAME: " + results['files'][0]['name'] + " ID: " + results['files'][0]['id'])

#            sleep(4)
            sleep(1)
            # 昔はファイル名で開いてました
#            worksheet = gc.open(SPREADSHEET_NAME).worksheet("集計シート")
            #Tenjinのapp_id検索して開く
            workbook = gc.open_by_key(SPREADSHEET_KEY)
            worksheet = workbook.worksheet("集計シート")

            # ファイル名変更確認
            if workbook.title != SPREADSHEET_NAME:
                print('ファイル名書き換え処理〜')
                with open(LOG, mode='a') as f:
                    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_aggregate_tt : スプレッドシート名更新 : " + str(workbook.title) + " : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

                sleep(1)
                service.files().update(
                        fileId=SPREADSHEET_KEY,
                        body={'name': SPREADSHEET_NAME}
                        ).execute()

            if DEBUG:
                print("ファイルオープン成功")

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_aggregate_tt : API制限 ファイルオープン失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 ファイルオープン失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            print(aggregate)
            print(type(e))
            print(str(e))
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_aggregate_tt : ファイル作成 " + SPREADSHEET_NAME + "\n")

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

            # Tenjin_app_id書き込み
            sleep(2)
            app_id_cells = worksheet.range(4, 2, 5, 2)
            app_id_cells[0].value='Tenjin app_id'
            app_id_cells[1].value=event['app_id']
            sleep(2)
            worksheet.update_cells(app_id_cells, value_input_option='USER_ENTERED')

        # 当日行取得、無ければ作る
        try:
#            sleep(3)
            sleep(1)
            target = worksheet.find(str(CHECK_DATE))
#            sleep(3)
            sleep(1)
            target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 51)
            target_cells[2].value=aggregate['app_name']

        # 無いので行を追加
        except gspread.exceptions.CellNotFound as e:
            if DEBUG:
                print(type(e))
                print("新規追加")
                print(e)

            # 売上集計シートに行追加
#            sleep(4)
            sleep(3)
            sales_summary = gc.open(SPREADSHEET_NAME).worksheet("売上集計")
            # リファレンス行をコピー
#            sleep(4)
            sleep(3)
            reference_list = sales_summary.row_values(11, value_render_option='FORMULA')

            # 最終行にペースト
            del reference_list[0]
#            sleep(4)
            sleep(3)
            sales_summary.append_row(reference_list, value_input_option='USER_ENTERED')

            # 書き込みデータ作成
            target_list=['']*3
            target_list[1]=str(CHECK_DATE)
            target_list[2]=aggregate['app_name']

            # 最終行に追加
#            sleep(4)
            sleep(3)
            worksheet.append_row(target_list, value_input_option='USER_ENTERED')

            # 最終行に日付追加
#            sleep(4)
            sleep(3)
            target = worksheet.find(str(CHECK_DATE))

#            sleep(4)
            sleep(3)
            target_cells = worksheet.range(target.row, target.col - 1, target.row, target.col + 51)

        except gspread.exceptions.APIError as e:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_aggregate_tt : API制限 当日行処理失敗 スキップ : " + str(CHECK_DATE) + " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print(type(e))
                print("API制限 当日行処理失敗 スキップ " + SPREADSHEET_NAME)
            continue

        except Exception as e:
            print(type(e))

        # revenueゼロ埋め
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

        # revenue_detailゼロ埋め
        target_cells[3].value=0
        target_cells[4].value=0
        target_cells[6].value=0
        target_cells[7].value=0

         # spendゼロ埋め
        target_cells[17].value=0
        target_cells[18].value=0
        target_cells[19].value=0
        target_cells[20].value=0
        target_cells[21].value=0
        target_cells[22].value=0
        target_cells[23].value=0
        target_cells[24].value=0
        target_cells[25].value=0
        target_cells[26].value=0
        target_cells[30].value=0
        target_cells[31].value=0
        target_cells[32].value=0
        target_cells[33].value=0
        target_cells[32].value=0
        target_cells[33].value=0
        target_cells[36].value=0
        target_cells[37].value=0
        target_cells[38].value=0
        target_cells[39].value=0
        target_cells[40].value=0
        target_cells[41].value=0
        target_cells[42].value=0
        target_cells[43].value=0
        target_cells[44].value=0
        target_cells[45].value=0
        target_cells[48].value=0
        target_cells[49].value=0
        target_cells[50].value=0
        target_cells[51].value=0

        # DAUゼロ埋め
        target_cells[27].value=0
        target_cells[28].value=0

        # purchaseゼロ埋め
        target_cells[46].value=0

    if aggregate['revenue'] > 0:
        REVENUE=aggregate['revenue']
        if DEBUG:
            print(aggregate['app_name'] + " : " + aggregate['platform'] + " : " + aggregate['ad_name'] + " : " + " : 広告収入 $" + str(REVENUE))

        # Unity Ads収入
        if aggregate['ad_name'] == 'Unity Ads':
            target_cells[5].value=REVENUE

        # nend収入
    #    if aggregate['ad_name'] == '':
    #        worksheet.update_cell(target.row, 9, REVENUE)

        # Ad Generation収入
        elif aggregate['ad_name'] == 'Ad Generation':
            target_cells[9].value=REVENUE

        # FIVE収入
        elif aggregate['ad_name'] == 'FIVE':
            target_cells[10].value=REVENUE

        # Maio収入
        elif aggregate['ad_name'] == 'Maio':
            target_cells[11].value=REVENUE

        # i-mobile Affiliate収入
        elif aggregate['ad_name'] == 'i-mobile Affiliate':
            target_cells[12].value=REVENUE

        # ironSource-Publisher収入
        elif aggregate['ad_name'] == 'ironSource-Publisher':
            target_cells[14].value=REVENUE

        # Tapjoy収入
        elif aggregate['ad_name'] == 'Tapjoy':
            target_cells[15].value=REVENUE

        # Facebook Audience Network収入
        elif aggregate['ad_name'] == 'Facebook Audience Network':
            target_cells[16].value=REVENUE

        # Vungle収入
        elif aggregate['ad_name'] == 'Vungle':
            target_cells[29].value=REVENUE

        # TikTok Audience Network収入
        elif aggregate['ad_name'] == 'TikTok Audience Network':
            target_cells[34].value=REVENUE

        # Mintegral Publisher収入
        elif aggregate['ad_name'] == 'Mintegral Publisher':
            target_cells[35].value=REVENUE

        # inmobi収入
        elif aggregate['ad_name'] == 'inmobi':
            target_cells[47].value=REVENUE

        elif (aggregate['ad_name'] == 'Google AdMob banner') and (aggregate['ad_name'] == 'Google AdMob interstitial') and (aggregate['ad_name'] == 'Applovin banner') and (aggregate['ad_name'] == 'Applovin interstitial'):
            pass

        else:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_aggregate_tt : 新規revenueアドネットワーク : " + str(aggregate['ad_name']) + " : " + str(REVENUE) +  " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print("新規revenueアドネットワーク : " + str(aggregate['ad_name']) + " : " + str(REVENUE))

    if aggregate['revenue'] > 0:
        REVENUE_DETAIL=str(aggregate['revenue'])

        if DEBUG:
            print(aggregate['app_name'] + " : " + aggregate['platform'] + " : " + aggregate['ad_name'] + " : " + " : 広告詳細収入 $" + str(REVENUE_DETAIL))

        # Google AdMob banner収入
        if aggregate['ad_name'] == 'Google AdMob banner':
            target_cells[3].value=REVENUE_DETAIL

        # Google AdMob interstitial収入
        elif aggregate['ad_name'] == 'Google AdMob interstitial':
            target_cells[4].value=REVENUE_DETAIL

        # Applovin banner収入
        elif aggregate['ad_name'] == 'Applovin banner':
            target_cells[6].value=REVENUE_DETAIL

        # Applovin interstitial収入
        elif aggregate['ad_name'] == 'Applovin interstitial':
            target_cells[7].value=REVENUE_DETAIL


    if aggregate['spend'] > 0:
        SPEND=aggregate['spend']

        if DEBUG:
            print(aggregate['app_name'] + " : " + aggregate['platform'] + " : " + aggregate['ad_name'] + " : インストール " + str(aggregate['install'] or 0) + " : 広告支出 $" + str(SPEND))

        # Applovin出稿
        if aggregate['ad_name'] == 'Applovin':
            target_cells[17].value=aggregate['install']
            target_cells[18].value=SPEND

        # Tapjoy出稿
        elif aggregate['ad_name'] == 'Tapjoy':
            target_cells[19].value=aggregate['install']
            target_cells[20].value=SPEND

        # Unity出稿
        elif aggregate['ad_name'] == 'Unity Ads':
            target_cells[21].value=aggregate['install']
            target_cells[22].value=SPEND

        # Facebook出稿
        elif aggregate['ad_name'] == 'Facebook':
            target_cells[23].value=aggregate['install']
            target_cells[24].value=SPEND

        # ironSource出稿
        elif aggregate['ad_name'] == 'ironSource':
            target_cells[25].value=aggregate['install']
            target_cells[26].value=SPEND

        # Google Ads出稿
        elif aggregate['ad_name'] == 'Google Ads':
            target_cells[30].value=aggregate['install']
            target_cells[31].value=SPEND

        # TikTok出稿
        elif aggregate['ad_name'] == 'TikTok':
            target_cells[32].value=aggregate['install']
            target_cells[33].value=SPEND

        # Toutiao出稿
        elif aggregate['ad_name'] == 'Toutiao':
            target_cells[32].value=aggregate['install']
            target_cells[33].value=SPEND

        # Snapchat出稿
        elif aggregate['ad_name'] == 'Snapchat':
            target_cells[36].value=aggregate['install']
            target_cells[37].value=SPEND

        # Mintegral出稿
        elif aggregate['ad_name'] == 'Mintegral':
            target_cells[38].value=aggregate['install']
            target_cells[39].value=SPEND

        # Apple Search Ads出稿
        elif aggregate['ad_name'] == 'Apple Search Ads':
            target_cells[40].value=aggregate['install']
            target_cells[41].value=SPEND

        # Maio出稿
        elif aggregate['ad_name'] == 'Maio':
            target_cells[42].value=aggregate['install']
            target_cells[43].value=SPEND

        # i-mobile Affiliate出稿
        elif aggregate['ad_name'] == 'i-mobile Affiliate':
            target_cells[44].value=aggregate['install']
            target_cells[45].value=SPEND

        # inmobi出稿
        elif aggregate['ad_name'] == 'inmobi':
            target_cells[48].value=aggregate['install']
            target_cells[49].value=SPEND

        # Adeal出稿
        elif aggregate['ad_name'] == 'Adeal':
            target_cells[50].value=aggregate['install']
            target_cells[51].value=SPEND

        else:
            with open(LOG, mode='a') as f:
                f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_aggregate_tt : 新規spendアドネットワーク : " + str(aggregate['ad_name']) + " : " + str(SPEND) +  " : " + SPREADSHEET_NAME + "\n")

            if DEBUG:
                print("新規spendアドネットワーク : " + str(aggregate['ad_name']) + " : " + str(SPEND))

    # DAU
    if aggregate['ad_name'] == '':
        if DEBUG:
            print(aggregate['app_name'] + " : " + aggregate['platform'] + " : " + aggregate['ad_name'] + " : DAU " + str(aggregate['dau']) + " : インストール " + str(aggregate['install']))

        target_cells[27].value=(aggregate['dau'] or 0)
        target_cells[28].value=(aggregate['install'] or 0)

    if aggregate['purchase'] > 0:
        if DEBUG:
            print(aggregate['app_name'] + " : " + aggregate['platform'] + " : " + aggregate['ad_name'] + " : 課金収入 $" + str(aggregate['purchase']))

        target_cells[46].value=aggregate['purchase']

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": check_aggregate_tt end\n")

