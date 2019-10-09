#!/usr/bin/python3.7
# -*- coding: utf8 -*-

import sys

from oauth2client.service_account import ServiceAccountCredentials

from googleapiclient.discovery import build
import httplib2

args = sys.argv

LOG='/tmp/superset.log'
FILE_ID=args[1]

# Googleスプレッドシート削除
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('gcp.json', scope)

http = httplib2.Http()
http = credentials.authorize(http)
service = build('drive', 'v3', http=http)

service.files().delete(fileId=FILE_ID).execute()
service.files().emptyTrash().execute()
