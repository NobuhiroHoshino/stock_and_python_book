# -*- coding: utf-8 -*-
from pyquery import PyQuery

def testa():
    q = PyQuery('https://kabutan.jp/stock/?code=7203')
    sector = q.find('#stockinfo_i2 > div > a')[0].text
    tai = q.find('#stockinfo_b1 .kubun_btn[data-win="#kubun_win1"]').text()
    print(sector)
    print(tai)

testa()