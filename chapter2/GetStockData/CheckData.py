import sqlite3
import pandas as pd

def CheckOneBrand(codes):
# codesは銘柄のリスト

    dbFileName = r'C:\Users\Nobuhiro Hoshino\Documents\stock\StockPrices.db'
    conn = sqlite3.connect(dbFileName)

    #営業日取得。priceデータから日付情報だけをソートして抽出して重複を削除すれば、営業日情報になる。（さすがに１銘柄も情報のない営業日はないと信じたい）
    sql='SELECT DISTINCT date FROM prices ORDER BY date ASC;'
    dates = pd.read_sql(sql, conn)
    dates.to_csv('businessday.csv')

    for code in codes:
        sql='SELECT * FROM prices WHERE code="' + code +'" ORDER BY date ASC;'
        brandData = pd.read_sql(sql, conn)

        #直近データが取れていないことにこれは気づけない。
        # brandsデータの日付でループしているので。。
        oldestDate = brandData.iloc[0, 0]
        la = len(dates.query('date >= "' + oldestDate + '"'))
        lb = len(brandData.query('date >= "' + oldestDate + '"'))
        if la != lb:
            #営業日データに不整合あり（単に新しいデータの可能性あり）
            print(code, 'NG', lb)

            # 営業日データと、銘柄データの日付を突き合わせる
            j = 0
            for i in range(lb):
                if dates.iloc[j, 0] != brandData.iloc[i, 0]:
                    print(dates.iloc[j, 0], code)
                    while dates.iloc[j, 0] != brandData.iloc[i, 0] and j <= la:
                        j += 1
                j += 1

        #     これだと、brandsデータが最新でないと、直近取り漏れがあると判定できない。

        else:
            print(code, 'ok')

    conn.close()

def CheckBrands():
    codes=['1301']
    CheckOneBrand(codes)

CheckBrands()