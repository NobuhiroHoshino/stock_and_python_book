#https://non-dimension.com/kabuka-scraping/
#BS4でデータフレームに入れてから銘柄ごとにまとめてCSVにしている。
#これはCSVではなくて、スクレイピングしている。
#これをもとに、 SQLiteにデータを放り込む

#株価履歴格納用のテーブルを作成した。
#修正後終値はadjustedcloseとした

#CSV同様、SQLiteにdataframeを書き込みする
#CSVは銘柄別ファイルとする
#CSV読み込み時には、もし既存CSVファイルがあればこれを開いて、最終データを取得して
#その次の日のデータから追記していく。
#ファイルがなければ一気に書き込みする。
#初回用と更新用はコード分けてもいいかもしれない。

#銘柄別にDBが分かれていないのはちょっと気持ち悪いが
#テーブル設計は別途のソフトで行う

from bs4 import BeautifulSoup
import pandas as pd
# import pandas.io.sql 記述しなくてもいいらしい
import requests
from datetime import datetime
import time
import sqlite3
import ctypes

def get_dfs(stock_number, year):
#stocknumberはコード番号
#yearは必要な年のリスト
    dfs = []
    for y in year:
        try:
            print(y)
            url = 'https://kabuoji3.com/stock/{}/{}/'.format(stock_number,y)
            hdrs = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
            }
            soup = BeautifulSoup(requests.get(url, headers=hdrs).content,'html.parser')
            #forbiddenエラー発生。userAgent設定したらセーフ
            #https://non-dimension.com/solution-403forbidden/

            tag_tr = soup.find_all('tr')
            head = [h.text for h in tag_tr[0].find_all('th')]

            data = []
            for i in range(1,len(tag_tr)):
                data.append([d.text for d in tag_tr[i].find_all('td')])
            df = pd.DataFrame(data, columns = head)

#            col = ['始値','高値','安値','終値','出来高','終値調整']
#            for c in col:
#                df[c] = df[c].astype(float)
#            df['日付'] = [datetime.strptime(i,'%Y-%m-%d') for i in df['日付']]
            dfs.append(df)
            time.sleep(2)
        except IndexError:
            print('No data',stock_number,y)
            time.sleep(1)
    return dfs

def concatenate(dfs):
    data = pd.concat(dfs,axis=0)    #pandasのdataframe結合
    data = data.reset_index(drop=True)  #index 振り直し
#floatに変換してもSQLiteはそこまで細かな型指定できないんで。
 #   col = ['始値','高値','安値','終値','出来高','終値調整']
 #   for c in col:
 #       data[c] = data[c].astype(float)
    return data

#作成した銘柄リストをSQLiteのbrandsテーブルから読み込む
def get_price_dataframe(db_file_name):
    conn = sqlite3.connect(db_file_name)
    return pd.read_sql('SELECT code, name FROM brands ORDER BY code',conn)

def AMain():
    Debug=True
    if Debug:
        SQLFILE=r'..\testStockPrices.db'
        CSVPATH=r'test'
    else:
        SQLFILE=r'C:\Users\Nobuhiro Hoshino\Documents\stock\StockPrices.db'
        CSVPATH='C:\\Users\\Nobuhiro Hoshino\\Documents\\stock\\CSV\\'

    cl=get_price_dataframe(SQLFILE)
    code_list=pd.DataFrame(cl,columns=['code', 'name'])

    #１日５００銘柄とする。およそ１０時間。４０５９銘柄あるので、８日間で終わる
    listnumber=len(code_list)
    dayAll=range(listnumber)
    day0=range(0,3) #debug用データ作成
    day1=range(0,500)
    day12=range(174,500)
    day2=range(500,1000)    #終了
    day3=range(1000,1500)   #終了0623 およそ10時間かかる。
    day4=range(1500,2000)   #終了0624
    day5=range(2000,2500)   #終了0625
    day6=range(2500,3000)   #終了0626
    day7=range(3000,3500)   #終了0627
    day8=range(3500,listnumber)

    #ここで変数範囲を選択。あとは手動で変える
    todaylist=day0

    # ファイルが存在する場合は、上書きされる
    conn = sqlite3.connect(SQLFILE)
    cursor = conn.cursor()
    if (todaylist == day1) or Debug:
        cursor.execute('DELETE FROM prices')  # 初回前提なんで、テーブルのデータは全削除

    for i in todaylist:
        k = code_list.loc[i,'code']
        v = code_list.loc[i,'name']
        print(k,v)
        dfs = get_dfs(k,range(1983,2020))   #debug用に2019年末までにする。
        if dfs:
            data = concatenate(dfs)

            #dataframeのcolを変える。renameならdictで変更前、変更後を設定する。ピンポイントで変更が可能。
            #data.columns=['date','open'・・・]でリストを入れれば、丸ごとの変更が可能。
            renameindex = {'日付':'date','始値':'open','高値':'high' ,
                           '安値':'low','終値':'close','出来高':'volume','終値調整':'adjustedclose'}
            data=data.rename(columns= renameindex) #renameは本体を書き換えないので

            #code列がないんで追加が必要
            #順番にこだわらなければ、代入すれば勝手にできる。便利。いろいろやり方はあるが、位置指定できるのは、insert
            data.insert(1,'code',k)#insertは本体を書き換える。ついでにcodeで埋められる。便利。

            data.to_csv('{}{}-{}.csv'.format(CSVPATH, k, v), header=True, index=False)

            #dataframeをSQLiteに一気に書き出す
            data.to_sql('prices',conn,if_exists='append', index=None)
            #appendはテーブルがある場合は追加。replaceはそっくりテーブルを書き換えてしまう模様。
            #また、dataframeはfloatらしいので、格納するとき型がどうなるか不安

            #処理が終わった番号を出力。これがわかればこの次から再開できる。
            print('completed-{}'.format(i))
            print(datetime.now().isoformat())

    # Windowsのスリープ
    if not Debug:
        ctypes.windll.PowrProf.SetSuspendState(0, 1, 0)

AMain()