# とりあえず３００日分は問答無用でスクレイピングしてしまう。
# あとは、DBとCSVを見ながら３００日データから追加していく

from bs4 import BeautifulSoup
import pandas as pd
import requests
from datetime import datetime
import time
import sqlite3
import platform


def get_df(stock_number):
    # stocknumberはコード番号
    try:
        # URLは年を指定しなければ300日最新が表示される
        # サイト側のデータ構造は年データとまったく一緒なので、他はいじらないでもデータを取れるはず。
        url = 'https://kabuoji3.com/stock/{}/'.format(stock_number)
        hdrs = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
        }
        soup = BeautifulSoup(requests.get(url, headers=hdrs).content, 'html.parser')

        tag_tr = soup.find_all('tr')
        head = [h.text for h in tag_tr[0].find_all('th')]

        data = []
        for i in range(1, len(tag_tr)):
            data.append([d.text for d in tag_tr[i].find_all('td')])
        df = pd.DataFrame(data, columns=head)

        time.sleep(2)
    except IndexError:
        print('No data ', stock_number)
        time.sleep(1)

    return df


# 作成した銘柄リストをSQLiteのbrandsテーブルから読み込む
def get_price_dataframe(db_file_name):
    conn = sqlite3.connect(db_file_name)
    return pd.read_sql('SELECT code, name FROM brands ORDER BY code', conn)


def AMain():
    debug=True

    SQLFILE = r'C:\Users\Nobuhiro Hoshino\PycharmProjects\stock_and_python_book\StockPrices.db'
    CSVPATH = 'C:\\Users\\Nobuhiro Hoshino\\Documents\\stock\\CSV\\'

    if debug:
        SQLFILE = r'..\testStockPrices.db'
        CSVPATH= 'test'

    cl = get_price_dataframe(SQLFILE)
    code_list = pd.DataFrame(cl, columns=['code', 'name'])

    todaylist = range(len(code_list))
    if debug:
        todaylist=range(0,1)

    renameindex = {'日付': 'date', '始値': 'open', '高値': 'high',
                   '安値': 'low', '終値': 'close', '出来高': 'volume', '終値調整': 'ajustedclose'}

    conn = sqlite3.connect(SQLFILE)
    # cursor = conn.cursor()

    for i in todaylist:
        k = code_list.loc[i, 'code']
        v = code_list.loc[i, 'name']
        print(k, v)
        data = get_df(k)
        #ここまでで、データは取れているのは確認した。ちなみに、データは新しい→古いの順番。３００日データは順番が逆
        if not data.empty: #DFの空判定は、if DF:ではだめな模様
            data = data.rename(columns=renameindex)

            # pandasで読み込み。
            csvname='{}{}-{}.csv'.format(CSVPATH, k, v)
            csvdata=pd.read_csv(csvname)
            lastdate=csvdata.iloc[-1]['日付']      #最終データの日付を調べる
            lastdate='date > "' + lastdate +'"'     #文字列は"でかこまないと。
            adddata = data.query(lastdate,engine='python')
            adddata = adddata.sort_values('date',ascending=True)    # このまま追記すると、データの順番が逆になるので
            adddata.to_csv('{}{}-{}.csv'.format(CSVPATH, k, v), mode='a', header=None,index=None)    #追加モード


            # priceテーブルから、銘柄Kの最新レコードを取り出す。
            # dataのその日付の次のデータを見つける（これはCSVと一致することでよいか？）
            # そこから先のデータの追加を行う。
            # データフレームの不要部分を削除してやれば、一気に追加できると思われる。

            adddata.insert(1, 'code', k)
            data.to_sql('prices', conn, if_exists='append', index=None)


AMain()
