
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
import time

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
    cur = conn.cursor()
    #with conn:
    #    sql = 'INSERT INTO new_brands(code,date) VALUES(?,?)'   #文字列連結で頑張るより、こちらのほうがスマートな処理かも。
    #    conn.executemany(sql, new_brands_generator())   #当たり前だが、これだと重複しまくる。同じデータの判定が必要。ループ処理が必要

    newdata = GetNewBrands()    #最新の追加銘柄を一括取得
    for code, date in zip(newdata['code'],newdata['date']):
        sql='SELECT code, date FROM new_brands WHERE code="' + code + '" AND date="' + date +'"'
        samedata = pd.read_sql(sql, conn)  # 同一データ抽出
        if samedata.empty:
            adddata=pd.DataFrame([[code, date]], columns=head) #dataframeにするのに、[[]]でないとダメだった。
            adddata.to_sql('new_brands', conn, if_exists='append', index=None)  #同じデータがなければ書き込み

            AddNewBrandToTable(conn, code)   #brandsテーブルへの書き込み

            # ３００日データを取得するならまあ初回データは不要だが

            #市場変更はどうするか？すでに銘柄がある場合があるのではないか。４ページ、複数パータンあり
            #kabutanで銘柄更新かけたほうが早いのではないか

    #その次は削除銘柄
    #リストに乗ってから実際に削除されるまでに時間がある
    #削除された銘柄の、brandsテーブルとpriceテーブルでの取り扱い
    #テーブルから削除したら、削除済のフラグは必要。削除銘柄一覧。。

def get_brand(code):
  url = 'https://kabutan.jp/stock/?code={}'.format(code)

  q = PyQuery(url)

  if len(q.find('div.company_block')) == 0:
    return None

  try:
    #CSSセレクタで、直接HTMLの要素を指定していると思われる。
    #元の画面構成が変わったら変更しなければいけないんだろうね
    name = q.find('div.company_block > h3').text()
    code_short_name =  q.find('#stockinfo_i1 > div.si_i1_1 > h2').text()
    short_name = code_short_name[code_short_name.find(" ") + 1:]
    market = q.find('span.market').text()
    unit_str = q.find('#kobetsu_left > table:nth-child(4) > tbody > tr:nth-child(6) > td').text()
    unit = int(unit_str.split()[0].replace(',', ''))
    sector = q.find('#stockinfo_i2 > div > a').text()
  except (ValueError, IndexError):
    return None

  return code, name, short_name, market, unit, sector

#code_rangeに入れたリストで、上記の銘柄情報取得のループを回す。
def brands_generator(code_range):
  for code in code_range:
    brand = get_brand(code)
    if brand:
      yield brand
    time.sleep(1)

def insert_brands_to_db(db_file_name, code_range):
  conn = sqlite3.connect(db_file_name)
  with conn:
    sql = 'INSERT INTO brands(code,name,shortname,market,unit,sector) ' \
          'VALUES(?,?,?,?,?,?)'
    conn.executemany(sql, brands_generator(code_range))

def mainGetAllBrands():
#  db = r"C:\Users\Nobuhiro Hoshino\PycharmProjects\stock_and_python_book\chapter2\StockPrices.db"
    db = r"testDB.db"  #debug用の株価インデックス
    insert_brands_to_db(db, range(1301,9999))

def AddNewBrandToTable(conn, code):
    brand = get_brand(code) #銘柄詳細情報取得
    if brand == None:
        return 'NoCode' # 銘柄詳細情報なし
    else:
        cur = conn.cursor()

        #単純なINSERTならVALUESを使ってもできたが、 WHERE以下の条件をつけようとすると動かない。
        #仕方がないので、やはり確認するクエリと書き込みするクエリを分離するか。
        #変数を使う場合、一つだとタプルにしないとダメな模様。
        sql = 'SELECT code FROM brands WHERE code=?'
        cur.execute(sql,(code,))
        c = cur.fetchall()  #select結果の取り出し
        if not c:   #データがなければ書き込み
            sql = 'INSERT INTO brands VALUES(?, ?, ?, ?, ?, ?)'
            cur.execute(sql, brand)
            conn.commit()
            f = 'Added'     #銘柄追加成功
        else:
            f = 'Exist'    #銘柄すでに登録済
        cur.close()
        return f

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
    # db='testDB.db'
    # insert_new_brands_to_db(db)
    AddNewBrandToTable('9984')

AMain()
