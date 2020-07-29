# -*- coding: utf8 -*-
from os import getenv

import pymysql
from pymysql.err import OperationalError
import requests
import json
import time
import datetime

mysql_conn = None
# 拉取账户贵金属数据
def reptile():
  url = "https://mybank.icbc.com.cn/servlet/AsynGetDataServlet?Area_code=0200&trademode=1&proIdsIn=130060000043%7C130060000044%7C130060000041%7C130060000123%7C130060000045%7C130060000046%7C130060000042%7C130060000125&isFirstTime=0&tranCode=A00500"
  
  headers = {
    "Cookie": "BIGipServermybank_XSQ61_80_pool_shuangzhan=755925258.20480.0000",
    "Accept": "*/*",
    "User-Agent": "GoldHelper/2.2.5 (iPhone; iOS 14.0; Scale/3.00)",
    "Accept-Language": "zh-Hans-CN;q=1, en-CN;q=0.9"
  }
  response = requests.get(url, headers=headers).text
  # print(response)
  result = json.loads(response)
  datas = result['market'];
  for data in datas:
    data['data_time'] = result['sysdate'];
    data['create_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S');
    data['update_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S');
  return datas;

def __get_cursor():
    try:
        return mysql_conn.cursor()
    except OperationalError:
        mysql_conn.ping(reconnect=True)
        return mysql_conn.cursor()

def main_handler(event, context):
    datas = reptile();
    global mysql_conn
    if not mysql_conn:
        mysql_conn = pymysql.connect(
        host        = getenv('DB_HOST', '<YOUR DB HOST>'),
        user        = getenv('DB_USER','<YOUR DB USER>'),
        password    = getenv('DB_PASSWORD','<YOUR DB PASSWORD>'),
        db          = getenv('DB_DATABASE','<YOUR DB DATABASE>'),
        port        = int(getenv('DB_PORT','<YOUR DB PORT>')),
        charset     = 'utf8mb4',
        autocommit  = True
        )

    with __get_cursor() as cursor:
        try:
            for data in datas:
                sql = "INSERT INTO `t_icbc_metal`(`upordown`, `lowmiddleprice`, `updown_d`, `buyprice`, `unit`, `prodcode`, " \
              "`ebuyprice`, `openprice_dr`, `openprice_dv`, `metalname`, `esellprice`, `metalflag`, `sellprice`, " \
              "`middleprice`, `updown_y`, `openprice_yr`, `topmiddleprice`, `currcode`, `data_time`, `create_time`, " \
              "`update_time`) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                data['upordown'], data['lowmiddleprice'], data['updown_d'], data['buyprice'], data['unit'],
                data['prodcode'],
                data['ebuyprice'], data['openprice_dr'], data['openprice_dv'], data['metalname'], data['esellprice'],
                data['metalflag'], data['sellprice'], data['middleprice'], data['updown_y'], data['openprice_yr'],
                data['topmiddleprice'], data['currcode'], data['data_time'], data['create_time'],
                data['update_time']);
                cursor.execute(sql);
            mysql_conn.commit();
        except:
            mysql_conn.rollback();
        # mysql_conn.close();
