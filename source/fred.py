from utils import *
import sql

import os, datetime
import pandas as pd
import numpy as np
from fredapi import Fred


source_list = ['FRED']

class FredIndex(object):
    def __init__(self):
        self.db = sql.SQL(db_name='fredindex',chunk_size=1000)


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

    def utils_get_price(self, code:str, page:int=2, source:str='FRED'):
        """ Return
            dataframe
        """
        def get_from_fred(ticker:str, page:int=0):
            ''' Return
            Output: [Date, Close]
            '''
            start = '1970-01-03'
            end   = datetime.datetime.today().strftime("%Y-%m-%d")

            fred = Fred(api_key= os.environ.get('FRED_KEY'))
            ret = fred.get_series(ticker,observation_start=start, observation_end=end)
            ret = pd.DataFrame(ret).reset_index(drop=False)
            ret = ret.rename(columns={'index':'Date', 0:'Close'}).dropna()
            ret = self.append_missing_trading_date(ret)
            return ret


        if source == 'FRED':
            pasring_func = get_from_fred
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
                df = self.utils_get_price(code=ticker, page=2)
                self.db.create(ticker=ticker, df=df)
        self.db.show_tables()

    def get_instance(self, ticker):
        df = self.db.get_by_ticker(ticker=ticker)
        return df



if __name__ == '__main__':
    fred_index = FredIndex()
    tickers = ['SP500']

    fred_index.create_instance(tickers=tickers)
    df = fred_index.get_instance(ticker = tickers[0])

    print(df.head())