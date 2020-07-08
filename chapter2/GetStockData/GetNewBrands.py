
# coding=utf-8

#encodingはrequestを使用することで回避した。
#しかし、うまく会社名が抽出できない。
#表が複雑な構成になっていて、かつろくなタグがついていないこと、
#yieldを使って断続的に抽出していることもあり、
#同一CSSセレクタで社名だけ抜く構文を思いつかず。
#まあコード番号だけわかれば一応なんとかはなるので、今回は法人名取得は見送り。。

from pyquery import PyQuery
import datetime
import sqlite3
from urllib import request
import pandas as pd


def GetNewBrands():
    url = 'http://www.jpx.co.jp/listing/stocks/new/index.html'
    resp = request.urlopen(url)
    html = resp.read().decode("utf-8")

    q = PyQuery(html)
    #q.find('tbody > tr > td > a:first'),　法人名抽出失敗のあと
#    for d,  i in zip(q.find('tbody > tr:even > td:eq(0)'),
#                    q.find('tbody > tr:even span')):
#        date = datetime.datetime.strptime(d.text, '%Y/%m/%d').date()
#        yield (i.get('id'), date)

    head = ['code', 'date']

    data = []
    for d, i in zip(q.find('tbody > tr:even > td:eq(0)'),
                    q.find('tbody > tr:even span')):
        date = d.text.replace('/','-')  #日付は文字列として扱う。本体データの区切りに合わせる
        data.append([i.get('id'), date])

    df = pd.DataFrame(data, columns=head)
    return df

def insert_new_brands_to_db(db_file_name):
    head = ['code', 'date']
    conn = sqlite3.connect(db_file_name)
    #with conn:
    #    sql = 'INSERT INTO new_brands(code,date) VALUES(?,?)'   #文字列連結で頑張るより、こちらのほうがスマートな処理かも。
    #    conn.executemany(sql, new_brands_generator())   #当たり前だが、これだと重複しまくる。同じデータの判定が必要。ループ処理が必要

    newdata = GetNewBrands()
    for code, date in zip(newdata['code'],newdata['date']):
        sql='SELECT code, date FROM new_brands WHERE code="' + code + '" AND date="' + date +'"'
        samedata = pd.read_sql(sql, conn)  # 同一データ抽出
        if samedata.empty:
            d=[[code, date]]
            adddata=pd.DataFrame(d, columns=head) #dataframeにするのに、[[]]でないとダメだった。
            adddata.to_sql('new_brands', conn, if_exists='append', index=None)

def pickUpNameTest():
    #for id, name, date in new_brands_generator():
    #    print(id, name, date)

    url = 'http://www.jpx.co.jp/listing/stocks/new/index.html'
    resp = request.urlopen(url)
    html = resp.read().decode("utf-8")

    q = PyQuery(html)
#    n=q.find('tbody > tr > td > a'),
    n=q.find('bg-even')
    print(n.text)

def AMain():
    db='testDB.db'
    insert_new_brands_to_db(db)

AMain()