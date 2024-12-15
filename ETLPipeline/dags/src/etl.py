import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from requests import Session
# from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
import logging
import time

# we can use yfinance session
class LimiterSession(LimiterMixin, Session):
   pass
YF_SESSION = LimiterSession(
   limiter=Limiter(RequestRate(2000, Duration.HOUR)),
   bucket_class=MemoryQueueBucket,
)
# YF_SESSION = None

# # Configure Logging
# log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../logs')
# log_dir = os.path.abspath(log_dir)

# logger.basicConfig(
#     level=logger.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[logger.FileHandler(os.path.join(log_dir,"ETL_debug.log")), 
#               logger.StreamHandler()],
# )
logger = logging.getLogger(__name__)

#===============Extract===============
class yfinanceExtractor(object):
    def __init__(self,session = None) -> None:
        self.session = session

    def fetch_single_ticker(self,ticker:str,start_date = None, end_date = None) -> pd.DataFrame:
        """fetch data for a single ticker"""
        try:
            result = yf.download(ticker, start=start_date, end=end_date,interval="1d",multi_level_index=False,session = self.session,threads=True,timeout = 10)
            if result.empty:
                logger.error(f"No data downloaded for {ticker}")
                return pd.DataFrame()
            else:
                result = result.reset_index()
                result['Ticker'] = ticker
                return result
        except Exception as e:
            logger.error(f"Error downloading ticker {ticker}: {e}")
            

    def fetch_data(self, tickers:list, start_date = None, end_date = None) -> pd.DataFrame:
        """Fetch data for multiple tickers"""
        if not isinstance(tickers,list): raise ValueError("Input a list of tickers")
        if len(tickers) == 0: return pd.DataFrame()
        ticker_str = " ".join(tickers)
        try:
            result = yf.download(ticker_str, start=start_date, end=end_date,
                                    interval="1d",group_by = "Ticker",
                                    session = self.session,threads=True,
                                    progress = False, timeout = 10)
            if result.empty: 
                logger.error(f"No data downloaded")
                return pd.DataFrame()
            else:
                result = result.stack(level=0).rename_axis(["Date","Ticker"]).reset_index(level=1).reset_index()
                return result
        
        except Exception as e:
            logger.error(f"Error downloading multiple tickers: {e}")
            raise 


    def retry_failed_tickers(self, failed_tickers:list, start_date = None, end_date = None, retries=2, delay=3):
        """Retry downloading data for each failed ticker inidividually"""
        all_data = []
        if len(failed_tickers) == 0: return pd.DataFrame()
        for ticker in failed_tickers:
            for attempt in range(retries):
                logger.info(f"Retrying ticker: {ticker}, attempt {attempt + 1}")
                data = self.fetch_single_ticker(ticker,start_date,end_date)
                if not data.empty:
                    all_data.append(data)
                    break
            time.sleep(delay)
        
        return pd.concat(all_data,axis=0)
    

def find_missing_tickers(all_tickers, df):
    """Find tickers that are missing from the downloaded data"""
    downloaded_tickers = df["Ticker"].unique()
    missing_tickers = [ticker for ticker in all_tickers if ticker not in downloaded_tickers]
    missing_tickers = list(set(missing_tickers))
    return missing_tickers

#==============Transform================
    
# expected columns and types
EXPECTED_COLUMNS = {
    "Ticker": str,
    "Date": "datetime64[ns]",
    "Adj Close": float,
    "Close": float,
    "High": float,
    "Low": float,
    "Open": float,
    "Volume": (int, float),  # accept int or float for volume in the raw data
}

def validate_columns(df):
    for column, expected_type in EXPECTED_COLUMNS.items():
        if column not in df:
            logger.error(f"Missing column: {column}")
            return False
        # Check for null values
        if df[column].isnull().any():
            logger.error(f"Column '{column}' contains missing values.")
            return False
        
        # Check data type
        if column == "Date":
            # Ensure the Date column is a datetime type
            if not pd.api.types.is_datetime64_any_dtype(df[column]):
                try:
                    df[column] = pd.to_datetime(df[column])
                    logger.info(f"Converted column '{column}' to datetime.")
                except Exception as e:
                    logger.error(f"Failed to convert column '{column}' to datetime: {e}")
                    return False
        else:
            # Check numeric or string type
            if not df[column].map(lambda x: isinstance(x, expected_type)).all():
                logger.error(f"Column '{column}' contains invalid types. Expected: {expected_type}.")
                return False
    logger.info("All columns validated successfully.")
    return True

def format_columns(df):
    result = df.copy()
    # reorder columns
    fields = ["Ticker", "Date", "Adj Close", "Close", "High", "Low", "Open", "Volume"]
    result = result[fields]
    # rename columns
    result.columns = ["ticker", "date", "adj_close", "close", "high", "low", "open", "volume"]
    # convert into string date
    result["date"] = result["date"].dt.strftime('%Y-%m-%d')
    return result


def check_price_range(df):
    """drop negative prices and volumes"""
    MAX_PRICE = 9999999
    MIN_PRICE = 0
    cond = ((df["adj_close"] < MIN_PRICE) | (df["adj_close"]>MAX_PRICE))
    print(f"Number of records with out-of-range AdjClose: {cond.sum()}")
    return df.loc[~cond]

def check_date_range(df):
    MIN_DATE = "1900-01-01"
    MAX_DATE = "2099-01-01"
    cond = ((df["date"] < MIN_DATE) | (df["date"] > MAX_DATE))
    print(f"Number of records with out-of-range Date: {cond.sum()}")
    return df.loc[~cond]

def drop_duplicates(df):
    return df.drop_duplicates(subset = ["ticker","date"],keep = "last")

def calculate_daily_returns(df):
    """calculate daily return"""
    df = df.sort_values(by = "date")
    df["ret_1d"] = df.groupby("ticker")["adj_close"].pct_change()
    df = df.sort_values(["ticker","date"])
    return df

def drop_missing(df, subset=["ticker", "date", "adj_close", "close", "ret_1d"]):
    """
    Drops rows with missing values in critical fields.
    """
    before = len(df)
    df = df.dropna(subset=subset)
    logger.info(f"Dropped {before - len(df)} rows with missing values in {subset}.")
    return df


# ETL Transformation Pipeline
def transform_pipeline(df):
    """
    Executes the entire transformation pipeline in order.
    """
    try:
        # Validation
        valid = validate_columns(df)
        if not valid:
            raise ValueError(f"Validation Error")

        # Formatting
        df = format_columns(df)

        # Check ranges
        df = check_price_range(df)
        df = check_date_range(df)

        # Handle duplicates
        df = drop_duplicates(df)

        # Calculate returns
        df = calculate_daily_returns(df)

        # Handle missing values
        df = drop_missing(df)

        logger.info("Transformation pipeline completed successfully.")
        return df
    except Exception as e:
        logger.error(f"ETL Transformation failed: {e}")
        raise ValueError("Transformation failed")

#====================Load===================
class DataLoader(object):
    def __init__(self,url,conn_pool_size = 10, max_overflow = 20):
        self.url = url
        self.engine = create_engine(self.url,pool_size = conn_pool_size,max_overflow=max_overflow)
        # self.session_factory = sessionmaker(bind=self.engine)

    def clear_price_volume_table(self):
        """Refrain from using it"""
        sql_command = text("DROP TABLE IF EXISTS price_volume") # text("DELETE FROM price_volume;")
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    conn.execute(sql_command)
                print("Table cleared successfully.")
        except Exception as e:
            print(f"Error clearing table: {e}")
    
    def create_price_volume_table(self):
        # Create table command
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS price_volume (
            ticker VARCHAR(10) NOT NULL,            -- Stock ticker symbol
            date DATE NOT NULL,                     -- Date of the trading session
            open DECIMAL(20, 6),                    -- Opening price of the stock
            high DECIMAL(20, 6),                    -- Highest price of the stock
            low DECIMAL(20, 6),                     -- Lowest price of the stock
            close DECIMAL(20, 6),                   -- Closing price of the stock
            adj_close DECIMAL(20, 6),               -- Adjusted closing price
            volume BIGINT,                          -- Volume of shares traded
            ret_1d DECIMAL(20,6),                   -- daily return
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp for record creation
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp for updates
            PRIMARY KEY (ticker, date)
        );
        """
        
        # Create index command
        create_index_sql = "CREATE INDEX idx_ticker_date ON price_volume (ticker, date);"
     
        # Execute CREATE TABLE
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(text(create_table_sql))
                # Execute CREATE INDEX
                conn.execute(text(create_index_sql))


    def upsert_to_price_volume_table(self,data, do_dup_update = True):
        """Upsert records
        :do_dup_update: if we want to update on duplicated records
        for example, adj_close may be re-stated by yfinance if there are new company actions such as splits, dividends
        """

        data = data.to_dict(orient='records')

        if do_dup_update:
            insert_query = """
                INSERT INTO price_volume (ticker, date, adj_close, close, high, low, open, volume, ret_1d)
                VALUES (:ticker, :date, :adj_close, :close, :high, :low, :open, :volume, :ret_1d)
                ON DUPLICATE KEY UPDATE 
                    adj_close = VALUES(adj_close),
                    close = VALUES(close),
                    high = VALUES(high),
                    low = VALUES(low),
                    open = VALUES(open),
                    volume = VALUES(volume),
                    ret_1d = VALUES(ret_1d);
            """
        else:
            insert_query = """
            INSERT INTO price_volume (ticker, date, adj_close, close, high, low, open, volume, ret_1d)
            VALUES (:ticker, :date, :adj_close, :close, :high, :low, :open, :volume, :ret_1d)
            ON DUPLICATE KEY UPDATE ticker = ticker;
            """

        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    result = conn.execute(text(insert_query),data)
                    logger.info(f"Inserted {result.rowcount} rows out of {len(data)} records")
        except SQLAlchemyError as e:
            logger.error(f"Database error: {e}")
            

    def upsert_parallel_by_ticker(self,data, batch = 50, workers = os.cpu_count(), do_dup_update = True):
        tickers = data["ticker"].unique()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for i in range(0,len(tickers),batch):
                subset = data[data["ticker"].isin(tickers[i:i+batch])]
                if subset.empty: continue
                futures.append(executor.submit(self.upsert_to_price_volume_table, subset,do_dup_update))
            
            for future in as_completed(futures):
                future.result()  # Wait for task to finish

    def query_adjclose(self,tickers,start_date,end_date):
        query = """
        SELECT date,ticker,adj_close
        FROM price_volume
        WHERE ticker IN :ticker
        AND date BETWEEN :start_date AND :end_date
        """
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    query_result = conn.execute(text(query),{"ticker": tuple(tickers), "start_date": start_date, "end_date": end_date}).fetchall()
                    # need to convert format
                    data = [{"date": row[0].strftime('%Y-%m-%d'), "ticker": row[1], "adj_close": float(row[2])} for row in query_result]
            data = pd.DataFrame(data)
            return data
        except SQLAlchemyError as e:
            print(e)


            
def is_adjclose_updated(yfinanceObj:yfinanceExtractor,DataLoaderObj:DataLoader,tickers,start_date,end_date):
    """Check if adjClose price is restated"""
    # old adj close data
    old_data = DataLoaderObj.query_adjclose(tickers,start_date,end_date)

    # new data from yfinance
    new_data = yfinanceObj.fetch_data(tickers = tickers,start_date=start_date,end_date=end_date)
    # for simplicity, just reformat columns assuming the data quality is good
    new_data = format_columns(new_data)
    # compare old against new
    merged = pd.merge(new_data,old_data, on = ["date","ticker"],how = "left",suffixes=('_new', '_old'))
    differences = merged[(merged['adj_close_new'] - merged['adj_close_old']).abs()>1e-3]
    if not differences.empty:
        print(f"Adjusted close prices changed, recalculating returns.")
        return differences[["date","ticker"]].unique().tolist()
    else:
        return []



#============Piepline===========
def run_daily_pipeline(yfExtractObj:yfinanceExtractor,dataLoaderObj:DataLoader,tickers:list,start_date = None, end_date = None, **kwargs):
    """ETL piepline"""

    # download tickers in batch (yfinance provided multi-threading)
    df = yfExtractObj.fetch_data(tickers = tickers, start_date = start_date, end_date = end_date)
    
    # find missing tickers for retry
    missing_tickers = find_missing_tickers(tickers,df)
    # Push missing tickers to XCom
    if kwargs.get("ti") and missing_tickers:
        ti = kwargs["ti"]
        ti.xcom_push(key="missing_tickers", value=missing_tickers)
    
    # transform
    df = transform_pipeline(df)
    
    # insert into db in batch, if duplicated do not update
    dataLoaderObj.upsert_parallel_by_ticker(df,do_dup_update = False,batch=50)


#============= more modular pipeline ================
import os
import pandas as pd

def save_raw_data(yfExtractObj:yfinanceExtractor, tickers:list, start_date: str, end_date:str, landing_folder = None, **kwargs):
    """Download raw data and save it to a landing folder (usually in S3, by default save in local folder)"""
    
    # Fetch raw data
    df = yfExtractObj.fetch_data(tickers=tickers, start_date=start_date, end_date=end_date)
    
    # Save to landing folder in csv
    filename = str(kwargs["execution_date"]) + "raw_data.csv"
    if landing_folder is None:
        landing_folder = os.path.join(os.path.dirname(__file__),'..', '..','landing_zone',kwargs["dag"].dag_id,kwargs["task"].task_id)
    if not os.path.exists(landing_folder):
        os.makedirs(landing_folder)
    
    landing_file_path = os.path.join(landing_folder,filename)
    logger.info(f"Raw data saved to {landing_file_path}")
    df.to_csv(landing_file_path, index=False)

    # Push the raw data path to XCom for the next steps
    kwargs["ti"].xcom_push(key="raw_data_path", value=landing_file_path)
    return df

def get_missing_tikers(all_tickers,**kwargs):
    # Load raw data
    raw_data_path = kwargs["ti"].xcom_pull(key="raw_data_path")
    raw_df = pd.read_csv(raw_data_path)
    missing_tickers = find_missing_tickers(all_tickers,raw_df)
    return missing_tickers


def transform_and_save_data(processed_folder = None, **kwargs):
    """Load raw data, apply transformations, and save to processed folder"""
    
    # Load raw data
    raw_data_path = kwargs["ti"].xcom_pull(key="raw_data_path")
    raw_df = pd.read_csv(raw_data_path)
    
    # Apply transformations
    df = transform_pipeline(raw_df)
    
    # Save the processed data to the processed folder
    filename = str(kwargs["execution_date"]) + "processed_data.csv"
    if processed_folder is None:
        processed_folder = os.path.join(os.path.dirname(__file__),'..', '..','processed_zone',kwargs["dag"].dag_id,kwargs["task"].task_id)
    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder)
    processed_file_path = os.path.join(processed_folder, filename)

    df.to_csv(processed_file_path, index=False)
    logger.info(f"Processed data saved to {processed_file_path}")
    
    # Push the processed data path to XCom for the next steps
    ti = kwargs["ti"]
    ti.xcom_push(key="processed_data_path", value=processed_file_path)
    return df

def upsert_to_db(dataLoaderObj:DataLoader,**kwargs):
    """Upsert the processed data into the database"""
    
    # Load the processed data
    processed_data_path = kwargs["ti"].xcom_pull(key="processed_data_path")
    processed_df = pd.read_csv(processed_data_path)
    
    # Upsert into the database
    dataLoaderObj.upsert_parallel_by_ticker(processed_df, do_dup_update=False, batch=50)
    logger.info("Upserted processed data to database")
