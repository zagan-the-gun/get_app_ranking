#!/usr/bin/python3.7
# -*- coding: utf8 -*-

import requests
import json
from bs4 import BeautifulSoup
import os
import psycopg2
import datetime
import sys

args = sys.argv

DATABASE_URL='postgresql://'+ args[1] + ':' + args[2] + '@'+ args[3] + ':5439/'+ args[4]
LOG='/tmp/superset.log'
DEBUG=True
APP_ID=args[5]

def get_connection():
    return psycopg2.connect(DATABASE_URL)

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO superset_schema.app_ids(app_id, platform, created_at) SELECT %s, 1, CONVERT_TIMEZONE('JST', SYSDATE) WHERE NOT EXISTS ( SELECT app_id FROM superset_schema.app_ids WHERE app_id = %s); ", (APP_ID, APP_ID))

if DEBUG:
    print(APP_ID)

with open(LOG, mode='a') as f:
    f.write(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+": add_new_arrival_aos\n")

