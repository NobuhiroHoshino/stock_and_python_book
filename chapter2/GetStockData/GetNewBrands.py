
# -*- coding: utf-8 -*-

#現状の問題点
#Aタグが複数抽出されている。最初のAタグだけでいい場合、どう記述するのか
#encodingがどうも正しくない気がする。UTF-8ではないのか？


from pyquery import PyQuery
import datetime
import sqlite3

def new_brands_generator():
    url = 'http://www.jpx.co.jp/listing/stocks/new/index.html'
    q = PyQuery(url)
    for d, n, i in zip(q.find('tbody > tr:even > td:eq(0)'),
                    q.find('tbody > tr > td > a'),
                    q.find('tbody > tr:even span')):
        date = datetime.datetime.strptime(d.text, '%Y/%m/%d').date()
        print( q.find('tbody > tr > td > a'))
        yield (i.get('id'), n.text, date)

def insert_new_brands_to_db(db_file_name):
  conn = sqlite3.connect(db_file_name)
  with conn:
    sql = 'INSERT INTO new_brands(code,date) VALUES(?,?)'
    conn.executemany(sql, new_brands_generator())

def AMain():
    for id, name, date in new_brands_generator():
        print(id, name, date)

AMain()