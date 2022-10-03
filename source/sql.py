import chunk
from datetime import datetime
import pandas as pd
import sqlite3
import os



class SQL():
    def __init__(self, db_name:str, chunk_size=1000):
        self.db_name = db_name
        self.db_path = os.path.join(os.path.dirname(__file__),'..','fsdata', db_name+'.db')

        self.con = sqlite3.connect(self.db_path)
        self.chunk_size = chunk_size
        pass
    
    def check(self, ticker:str):
        cmd = 'SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'%s\';'%ticker
        ret = pd.read_sql(cmd, self.con, index_col=None)
        return not ret.empty

    def create(self, ticker:str, df):
        try:
            df.to_sql(ticker, self.con, chunksize=self.chunk_size)
        except:
            print("Ticker already exists [%s-%s]"%(self.db_name, ticker))

    def get_by_ticker(self, ticker:str):
        try:
            df = pd.read_sql("SELECT * FROM \"%s\""%(ticker), self.con, index_col='Date')
            df.index = pd.to_datetime(df.index)
            df.drop(['index'],axis=1,inplace=True)
        except:
            df = pd.read_sql("SELECT * FROM sqlite_master WHERE type='table';", self.con, index_col=None)
            print("Ticker not founded [%s-%s]"%(self.db_name, ticker))
            df = pd.DataFrame([])
        return df

    def delete(self, ticker:str):
        try:
            pass
        except:
            pass
    def show_tables(self):
        ret = pd.read_sql("SELECT * FROM sqlite_master WHERE type='table';", self.con, index_col=None)
        print(ret)

if __name__ == '__main__':

    raw_data = {'col0': [1, 2, 3, 4], 'col1': [10, 20, 30, 40], 'col2':[100, 200, 300, 400]}
    df = pd.DataFrame(raw_data)


    db_path = os.path.dirname(__file__)
    db_path = os.path.join(db_path+'test.db')
    con = sqlite3.connect(db_path)
    
    #df.to_sql('test', con, chunksize=1000)

