# とりあえず３００日分は問答無用でスクレイピングしてしまう。
# あとは、DBとCSVを見ながら３００日データから追加していく
#全銘柄で５時間くらいかかった
# 1613 東証電気機器株価指数連動型上場投資信託 上場廃止
# 8885 ラ・アトレ 株式併合
# 8044 大都魚類　
# 4217 日立化成　株式併合
# 3606 レナウン　民事再生
# 3258 ユニゾホールディングス　株式併合

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
        df = pd.DataFrame([])

    return df


# 作成した銘柄リストをSQLiteのbrandsテーブルから読み込む
def get_price_dataframe(db_file_name):
    conn = sqlite3.connect(db_file_name)
    return pd.read_sql('SELECT code, name FROM brands ORDER BY code', conn)


def AMain():
    Debug=False

    SQLFILE = r'C:\Users\Nobuhiro Hoshino\Documents\stock\StockPrices.db'
    CSVPATH = 'C:\\Users\\Nobuhiro Hoshino\\Documents\\stock\\CSV\\'

    if Debug:
        SQLFILE = r'..\testStockPrices.db'
        CSVPATH= 'test'

    print(datetime.now().isoformat())

    cl = get_price_dataframe(SQLFILE)
    code_list = pd.DataFrame(cl, columns=['code', 'name'])

    todaylist = range(len(code_list))
    if Debug:
        todaylist=range(0,1)

    renameindex = {'日付': 'date', '始値': 'open', '高値': 'high',
                   '安値': 'low', '終値': 'close', '出来高': 'volume', '終値調整': 'adjustedclose'}

    conn = sqlite3.connect(SQLFILE)
    for i in todaylist:
        k = code_list.loc[i, 'code']
        v = code_list.loc[i, 'name']
        print(i,k, v)
        newdata = get_df(k)    #戻り値はデータフレーム。DFのリストではない。
        #ここまでで、データは取れているのは確認した。ちなみに、データは新しい→古いの順番。３００日データは順番が逆
        if not newdata.empty: #DFの空判定は、if DF:ではだめな模様
            newdata = newdata.rename(columns=renameindex) #CSVもINDEXは日本語から英語に変えている。

            # CSV追記
            csvname = '{}{}-{}.csv'.format(CSVPATH, k, v)
            csvdata = pd.read_csv(csvname)            #まあ全データ読む必要はないかもしれないが
            lastdate = csvdata.iloc[-1]['date']         #最終データの日付を調べる
            lastdate = 'date > "' + lastdate +'"'      #文字列は"でかこまないと。query用の文字列
            adddata = newdata.query(lastdate,engine='python')  #これでlastadate以降のデータが抽出される。便利。
            if not adddata.empty:
                adddata = adddata.sort_values('date', ascending = True)    # このまま追記すると、データの順番が逆になるのでソート
                adddata.insert(1, 'code', k)    #CSV code入れないと駄目なんで。
                adddata.to_csv('{}{}-{}.csv'.format(CSVPATH, k, v), mode='a', header=None,index=None)    #追加モード

            # SQL追記
            lastdate = pd.read_sql('SELECT MAX(date) FROM prices WHERE code="'+ k +'"', conn)    #当該コードの最新日を抽出
            targetdate ='date > "' + lastdate.iloc[0,0] +'"'      #lastdateはpandas
            adddata = newdata.query(targetdate,engine='python')
            if not adddata.empty:
                adddata = adddata.sort_values('date', ascending = True)    # SQLなんでソートしなくてもいいけど
                adddata.insert(1, 'code', k)    #code入れないと駄目なんで。
                adddata.to_sql('prices', conn, if_exists='append',  index=None)

    print(datetime.now().isoformat())


AMain()
