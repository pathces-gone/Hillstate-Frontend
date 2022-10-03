from utils import *
import sql

import numpy as np
import pandas as pd
import datetime, os

from pandas_datareader import data as pdr
import yfinance as yf
import FinanceDataReader as fdr #pip3 install --user finance-datareader

# from fredapi import Fred
# from pyparsing import col


# import matplotlib.pyplot as plt

source_list = ['YAHOO', 'FDR']

class US(object):
    def __init__(self):
        self.db = sql.SQL(db_name='us',chunk_size=10000)

    def append_missing_trading_date(self, df:pd.core.frame.DataFrame):
        ''' Return
            input: 
                    Date	Close	Diff	Open	High	Low	Volume
            0	2022-02-07	10610	30	10605	10615	10585	72843
            output:
                    Date	Close	Diff	Open	High	Low	Volume	Trade
            0	2022-02-07	10610	30	10605	10615	10585	72843	True
        '''
        idx = pd.date_range(df.loc[:,'Date'].min().strftime('%Y-%m-%d'),df.loc[:,'Date'].max().strftime('%Y-%m-%d'))

        s = df.set_index('Date')
        s = s.reindex(idx, fill_value=0)

        s['Valid'] = s.apply(lambda x: x['Close'] != 0, axis=1)
        s = s.reset_index().rename(columns={"index": "Date"})

        for i in s.index:
            if s.iloc[i,-1]==False:
                s.iloc[i,1:-1] = s.iloc[i-1,1:-1]
        return s

    def utils_get_price(self, code:str, page:int=2, source:str='YAHOO'):
        """ Return
            dataframe
        """
        def get_from_yahoo(ticker:str, page=None):
            ''' Return
            Input : Date,[ Open, High, Low, Close, Adj Close, Volume]
            Output: [Date,Open, High, Low, Close, Adj Close, Volume, Trade]
            '''
            yf.pdr_override()
            df_price = pdr.get_data_yahoo(ticker)
            df_price = df_price.reset_index()

            df_price = self.append_missing_trading_date(df_price)
            return df_price

        if source == 'YAHOO':
            pasring_func = get_from_yahoo
        else:
            pasring_func = None

        ret = pasring_func(ticker=code, page=page)
        return  ret

    def create_instance(self,tickers):
        for ticker in tickers:
            if not self.db.check(ticker):
                df = self.utils_get_price(code=ticker, page=2)
                self.db.create(ticker=ticker, df=df)
        self.db.show_tables()

    def get_instance(self, ticker):
        df = self.db.get_by_ticker(ticker=ticker)
        return df
    
if __name__ == '__main__':
    us = US()
    tickers = ['TSLA', 'QQQ', 'SPY']

    #us.create_instance(tickers=tickers)
    #df = us.get_instance(ticker = tickers[1])
    us.create_instance(tickers=['KRW=X'])
    df = us.get_instance(ticker = 'KRW=X')

    print(df.iloc[::-1].head())