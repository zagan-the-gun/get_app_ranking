#!/usr/bin/python3.7
# -*- coding: utf8 -*-

import sys

from oauth2client.service_account import ServiceAccountCredentials

from googleapiclient.discovery import build
import httplib2

args = sys.argv

LOG='/tmp/superset.log'

# Googleスプレッドシート削除
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('gcp.json', scope)

http = httplib2.Http()
http = credentials.authorize(http)
service = build('drive', 'v3', http=http)

results = service.files().list(q="'1Z6nHs-LoO8D_HdXuY2wkH5yd2Uh70daP' in parents", pageSize=1000).execute()
for result in results['files']:
    print("NAME: " + result['name'] + " ID: " + result['id'])

for FILE_ID in results['files']:
    try:
        service.files().delete(fileId=FILE_ID['id']).execute()
        service.files().emptyTrash().execute()
    except:
        print("Delete error: " + FILE_ID['id'])

