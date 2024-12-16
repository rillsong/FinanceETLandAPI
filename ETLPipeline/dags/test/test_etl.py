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

