import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
import src.etl as ETL    
import pandas as pd

def test_fetch(yfobj:ETL.yfinanceExtractor,tickers,start_date,end_date):
    df = yfobj.fetch_single_ticker(ticker = "NVDA",start_date=start_date,end_date=end_date)
    ETL.save_raw_data(df,"single_ticker_sample.csv")

    df2 = yfobj.fetch_data(tickers = tickers,start_date=start_date,end_date=end_date)
    ETL.save_raw_data(df2,"multi_ticker_sample.csv")


def test_transform(df,tickers):
    df,missing_tickers = ETL.transform_pipeline(df,tickers)
    print(df.describe())
    print(len(missing_tickers))
    return df

def test_loader(dl:ETL.DataLoader,df):   
    dl.clear_price_volume_table()
    dl.create_price_volume_table() # run only once
    dl.upsert_to_price_volume_table(df)
    dl.upsert_parallel_by_ticker(df, batch=30,do_dup_update=True)

def test_check_adjclose(yfinanceObj,DataLoaderObj,tickers,start_date,end_date):
    result = ETL.is_adjclose_updated(yfinanceObj,DataLoaderObj,tickers,start_date,end_date)
    print(result)

def test_retry_tickers(yfExtractObj:ETL.yfinanceExtractor,missing_tickers):
    df = yfExtractObj.retry_failed_tickers(failed_tickers = missing_tickers, start_date = start_date, end_date = end_date, retries = 2)
    print(df.head())

if __name__=="__main__":
    path = os.path.join(os.getcwd(),"ETLPipeline","dags",r"tickers.csv")
    sp500_tickers = pd.read_csv(path,usecols=["Symbol"]).squeeze().tolist()
    start_date = '2024-11-10'
    end_date = '2024-12-10'
    
    DB_URL =  "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(**dict(user="root",password = "19990512",host="127.0.0.1",port = "3306", db = "MarketData"))
    dl = ETL.DataLoader(DB_URL)
    yfobj = ETL.yfinanceExtractor()

    # test_fetch(yfobj,sp500_tickers,start_date,end_date)
    df = pd.read_csv("multi_ticker_sample.csv")
    test_retry_tickers(yfobj,missing_tickers = ["NVDA"])
    df = test_transform(df,sp500_tickers)
    test_loader(dl,df)
    test_check_adjclose(yfobj,dl,sp500_tickers,start_date,end_date)