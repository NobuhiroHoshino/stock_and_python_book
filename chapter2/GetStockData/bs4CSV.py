#https://non-dimension.com/kabuka-scraping/
#BS4でデータフレームに入れてから銘柄ごとにまとめてCSVにしている。
#これはCSVではなくて、スクレイピングしている。

from bs4 import BeautifulSoup
import pandas as pd
import requests
from datetime import datetime
import time

def get_dfs(stock_number):
    dfs = []
    year = range(2018,2020)
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
    return dfs

def concatenate(dfs):
    data = pd.concat(dfs,axis=0)    #pandasのdataframe結合
    data = data.reset_index(drop=True)  #index 振り直し
#floatに変換してもSQLiteはそこまで細かな型指定できないんで。
 #   col = ['始値','高値','安値','終値','出来高','終値調整']
 #   for c in col:
 #       data[c] = data[c].astype(float)
    return data

#作成したコードリストを読み込む
#code_list = pd.read_csv('code_list.csv')
cl=[[1301,"極洋"]]
code_list=pd.DataFrame(cl,columns=['code', 'name'])

#複数のデータフレームをcsvで保存
#ほっとくとINDEXが入るので、パラメータで指定する
#ファイルが存在する場合は、上書きされる

for i in range(len(code_list)):
    k = code_list.loc[i,'code']
    v = code_list.loc[i,'name']
    print(k,v)
    dfs = get_dfs(k)
    data = concatenate(dfs)
    data.to_csv('{}-{}.csv'.format(k,v),header=True,index=False)