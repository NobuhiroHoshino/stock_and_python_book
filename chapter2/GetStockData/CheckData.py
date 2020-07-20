import sqlite3
import pandas as pd

def CheckOneBrand(codes):
# codesは銘柄のリスト

    dbFileName = r'C:\Users\Nobuhiro Hoshino\Documents\stock\StockPrices.db'
    conn = sqlite3.connect(dbFileName)

    #営業日取得。priceデータから日付情報だけをソートして抽出して重複を削除すれば、営業日情報になる。（さすがに１銘柄も情報のない営業日はないと信じたい）
    sql='SELECT DISTINCT date FROM prices ORDER BY date ASC;'
    dates = pd.read_sql(sql, conn)
    # dates.to_csv('businessday.csv')
    # dates = pd.read_csv('businessday.csv')

    cols = ['date', 'code']
    dfNoData = pd.DataFrame(index=[], columns=cols) #データがない日付と銘柄保存用の空DF

    for code in codes:
        sql='SELECT * FROM prices WHERE code="' + code +'" ORDER BY date ASC;'
        brandData = pd.read_sql(sql, conn)

        # ０列はdate列である前提。
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
                    record = pd.Series([dates.iloc[j, 0], code], index=dfNoData.columns)    #seriesにしてからDFに追加
                    dfNoData = dfNoData.append(record, ignore_index=True)   #index振り直し

                    while dates.iloc[j, 0] != brandData.iloc[i, 0] and j <= la:
                        j += 1
                j += 1

            newestDate = brandData.iloc[-1, 0]
            noDataDates = dates.query('date > "' + newestDate + '"').copy() # copyをつけないとスライスか別dfかわからんのでワーニング
            noDataDates['code'] = code
            dfNoData = dfNoData.append(noDataDates, ignore_index = True)

        else:
            print(code, 'ok')

    conn.close()
    print(dfNoData)

def CheckBrands():
    codes=['1301']
    CheckOneBrand(codes)

CheckBrands()