from utils import *
import sql

import pandas as pd
from pandas.core.frame import DataFrame
from pandas.core.series import Series

import requests, time, datetime, os
import numpy as np
from bs4 import BeautifulSoup as bs


source_list = ['NAVER']

class Korea(object):
    def __init__(self):
        self.db = sql.SQL(db_name='korea',chunk_size=10000)

    def utils_get_price(self, code:str, page:int=1000, source:str='NAVER'):
        """ Return
            dataframe
        """
        def get_from_naver(ticker:str, page:int):
            ''' Return
            Dataframe [Date	Close	Diff	Open	High	Low	Volume	Trade]
            '''
            print('Download - %s'%ticker)
            url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=ticker)
            def get_html_table(url:str):
                headers =  {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36'}
                response = requests.get(url, headers=headers)
                html = bs(response.text, 'lxml')
                html_table = html.select('table') 
                return str(html_table)

            df = pd.DataFrame()
            for _page in range(1, page):
                pg_url = '{url}&page={page}'.format(url=url, page=_page).replace(' ','')
                html_table = get_html_table(pg_url)
                df = pd.concat([df,pd.read_html(html_table, header=0)[0]],axis=0,ignore_index=True)
            assert df.empty == False, "the requested dataframe is empty."
            df = df.dropna(axis=0)

            df = df.rename(columns= {'날짜': 'Date', '종가': 'Close', '전일비': 'Diff', '시가': 'Open', '고가': 'High', '저가': 'Low', '거래량': 'Volume'}) 
            df[['Close', 'Diff', 'Open', 'High', 'Low', 'Volume']] = df[['Close', 'Diff', 'Open', 'High', 'Low', 'Volume']].astype(int) 
            df['Date'] = pd.to_datetime(df['Date']) 

            df = df.set_index('Date')

            df = df.loc[~df.index.duplicated(keep='first')] # 중복제거
            idx = pd.date_range(df.index.min(),df.index.max())

            s = df
            s = s.reindex(idx, fill_value=0)

            s['Valid'] = s.apply(lambda x: x['Close'] != 0, axis=1)
            s = s.reset_index()
            s= s.rename(columns={"index": "Date"})

            for i in s.index:
                if s.iloc[i,-1]==False:
                    s.iloc[i,1:-1] = s.iloc[i-1,1:-1] 

            ret = s.sort_values(by=['Date'], ascending=True)
            return ret

        if source == 'NAVER':
            pasring_func = get_from_naver
        else:
            pasring_func = None

        ret = pasring_func(ticker=code, page=page)
        return  ret


    def get_trading_date(self, ticker:str):
        path = os.path.join('..','fsdata',ticker+'.csv')
        if os.path.exists(path):
            price_df = pd.read_csv(path)
            return price_df['Date']
        else:
            return None


    def create_instance(self,tickers):
        for ticker in tickers:
            if not self.db.check(ticker):
                df = self.utils_get_price(code=ticker)
                self.db.create(ticker=ticker, df=df)
        self.db.show_tables()

    def get_instance(self, ticker):
        df = self.db.get_by_ticker(ticker=ticker)
        return df

if __name__ == '__main__':
    kor = Korea()
    tickers = ['411060', '005930','035720']


    kor.create_instance(tickers=tickers)
    df = kor.get_instance(ticker = tickers[3])

    print(df.head())