from fastapi import FastAPI, HTTPException, status,Query, Depends
from fastapi.responses import JSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from contextlib import asynccontextmanager
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram, Gauge
import asyncio
import redis.asyncio as redis
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
import re
import random
import json
import time
from functools import wraps

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app_debug.log"), logging.StreamHandler()],
)

# DB should be read-repeatable
DATABASE_URL = os.getenv("DATABASE_URL") # mysql+aiomysql
REDIS_URL = os.getenv("REDIS_URL")

# Creating DB connection
engine = create_async_engine(DATABASE_URL, pool_size=10, max_overflow=20)

# Define AsyncSessionLocal as my session factory
SessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

#==================Utility===================
# Define a custom key builder
def custom_key_builder(func, *args, **kwargs):
    # Use the endpoint name and parameters to build a key
    # @cache(expire=60, key_builder=custom_key_builder)  # Use custom key
    # fastapi-cache uses by default: "{function_name}:{arg1}:{arg2}:...:{argN}"
    # Build key with function name and path parameters
    request = kwargs.get("request")
    path_params = request.path_params if request else {}
    query_params = request.query_params if request else {}

    # Include function name, path params, and sorted query params
    key_parts = [
        func.__name__,
        *[f"{k}:{v}" for k, v in path_params.items()],
        *[f"{k}:{v}" for k, v in sorted(query_params.items())],
    ]
    return ":".join(key_parts)

# Prefetch hot keys into cache
async def prefetch_hot_keys(hot_tickers: list, days_back: int = 30):
    """
    Prefetch hot keys for the /returns endpoint.
    :hot_tickers (list): List of hot ticker symbols.
    :days_back (int): Number of days to fetch returns for (relative to today).
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=days_back)
    redis_client = FastAPICache.get_backend().redis  # Get Redis client from the backend

    for ticker in hot_tickers:
        try:
            # Cache the result with random expiration (to prevent stampedes)
            # cache_key = f"returns:{ticker}:{start_date.strftime('%Y-%m-%d')}:{end_date.strftime('%Y-%m-%d')}"
            cache_key = custom_key_builder(
                get_returns, 
                request=None,  
                ticker=ticker,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            
            # Check if data is already cached
            cached_value = await redis_client.get(cache_key)
            if cached_value:
                logging.info(f"Cache hit for {ticker} ({start_date} to {end_date})")
                continue  # Skip fetching if already cached

            # Query for prefetching
            query = """
            SELECT date, ticker, ret_1d
            FROM price_volume
            WHERE ticker = :ticker
            AND date BETWEEN :start_date AND :end_date
            """

            async with SessionLocal() as session:
                async with session.begin():  # Start a transaction
                    result = await session.execute(
                        text(query), {"ticker": ticker, "start_date": start_date, "end_date": end_date}
                    )
                    query_result = result.fetchall()
                    if not query_result:
                        logging.warning(f"No data found for {ticker} from {start_date} to {end_date}")
                    data = [{"date": row[0].strftime('%Y-%m-%d'), "ticker": row[1], "ret_1d": float(row[2])} for row in query_result]
                    # set cache    
                    await redis_client.setex(cache_key,random.randint(50, 300), json.dumps({"data": data})) # set+expire
                    logging.info(f"Prefetched and cached data for {ticker} from {start_date} to {end_date}")

        except Exception as e:
            logging.error(f"Error prefetching data for {ticker}: {e}")

HOT_TICKERS = ["AAPL", "GOOGL", "AMZN"]
CACHE_EXPIRATION = random.randint(50,300)
async def clear_cache_key(key: str):
    success = await FastAPICache.delete(key) # delete one key
    if success:
        return {"message": f"Cache for key '{key}' cleared."}
    else:
        raise HTTPException(status_code=404, detail="Cache key not found.")
    
async def clear_cache():
    """Clear the Redis cache."""
    await FastAPICache.clear() # delete all keys
    logging.info("Cache cleared.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Redis cache backend
    redis_client = redis.from_url(REDIS_URL)
    FastAPICache.init(RedisBackend(redis_client), prefix="fastapi-cache")
    logging.info("Cache initialized")
    # warm up
    days_back = 30  # Prefetch data for the past 30 days
    await prefetch_hot_keys(HOT_TICKERS,days_back = days_back)

    yield # running state
    
    # On shutdown, clear the cache
    await clear_cache()
    await redis_client.close()
    logging.info("Cache closed")

def track_metrics(func):
    @wraps(func)
    async def wrapper(*args,**kwargs):
        # start_time = time.time()
        response = None
        status_code = 200
        endpoint = kwargs.get("endpoint","")
        try:
            response = await func(*args,**kwargs)
            return response
        except HTTPException as e:
            status_code = e.status_code
            raise e
        finally:
            # # meature latency
            # duration = time.time() - start_time
            # # Increment request counter
            # REQUESTS.labels(method="GET", endpoint=endpoint, status_code=status_code).inc()
            # # Observe request latency
            # LATENCY.labels(method="GET", endpoint=endpoint).observe(duration)
            
            # # Measure and observe response size if response is not None
            # if response is not None:
            #     response_size = len(str(response).encode('utf-8'))  # Calculate response size in bytes
            #     RESPONSE_SIZE.labels(method="GET", endpoint=endpoint).observe(response_size)
            pass
    return wrapper

# Pydantic models for paging
class PaginatedResponse(BaseModel):
    total_items: int
    total_pages: int
    current_page: int
    items: list

# Initialize the Limiter
limiter = Limiter(key_func=lambda: "global")

# Initialize Prometheus Instrumentator
instrumentator = Instrumentator()
# Define Prometheus metrics
# REQUESTS = Counter("api_requests_total", "Total number of API requests", ["method", "endpoint", "status_code"])
# LATENCY = Histogram("api_request_duration_seconds", "Request latency in seconds", ["method", "endpoint"])
# RESPONSE_SIZE = Histogram("api_response_size_bytes", "Size of the responses", ["method", "endpoint"])

# Build API
app = FastAPI(lifespan=lifespan, title = "data_API",debug = True)
app.state.limiter = limiter # rate limiter
instrumentator.instrument(app).expose(app)
logging.info("Prometheus metrics initialized and exposed.")

def parse_tickers(tickers:str):
    if not tickers:
        raise HTTPException(status_code=400,detail="Provide at least one ticker name.")
    try:
        tickers_list = re.split(r'[,;\s]+', tickers) 
    except Exception as e:
         raise HTTPException(status_code=400,detail="Cannot parse string of tickers. Use delimiter , or ;")
    tickers_list = list(set([ticker.upper() for ticker in tickers_list])) # convert to upper case and drop duplicates
    return tickers_list

def parse_date(date:str):
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD'.")
    return date


# Endpoints
@app.get("/tickers",status_code=status.HTTP_200_OK)
@cache(expire=600,key_builder=custom_key_builder)  # Cache for 10 minutes
async def get_tickers():
    """Retrieve a list of all unique tickers from the price_volume table."""
    query = "SELECT DISTINCT ticker FROM price_volume"
    async with SessionLocal() as session:
        async with session.begin():  # Start a transaction
            try:
                result = await session.execute(text(query))
                result = result.fetchall()
                tickers = [row[0] for row in result]
                if not tickers:
                    raise HTTPException(status_code=404, detail="No data found")
                return {"tickers": tickers}

            except SQLAlchemyError as e:
                logging.error(f"Database error: {e}")
                raise HTTPException(status_code=500, detail="Error fetching tickers")



@app.get("/returns/{ticker}/{start_date}/{end_date}",status_code=status.HTTP_200_OK)
@cache(expire=CACHE_EXPIRATION,key_builder=custom_key_builder) # Cache for 50-300s
async def get_returns(ticker: str, start_date: str, end_date: str):
    """Calculate and return daily returns for a ticker within a date range."""

    start_date = parse_date(start_date)
    end_date = parse_date(end_date)
    ticker_list = parse_tickers(ticker)
    if len(ticker_list) > 1:
        raise HTTPException(status_code=400, detail="Only provide one ticker name.")
    
    query = """
    SELECT date, ticker, ret_1d
    FROM price_volume
    WHERE ticker = :ticker
    AND date BETWEEN :start_date AND :end_date
    """
    async with SessionLocal() as session:
        async with session.begin():  # Start a transaction
            try:
                result = await session.execute(
                    text(query), {"ticker": ticker, "start_date": start_date, "end_date": end_date}
                )
                query_result = result.fetchall()

                if not query_result:
                    raise HTTPException(status_code=404, detail="No data found for the given ticker and date range")

                data = [{"date": row[0].strftime('%Y-%m-%d'), "ticker": row[1], "ret_1d": float(row[2])} for row in query_result]
                return {"data": data}

            except SQLAlchemyError as e:
                logging.error(f"Database error: {e}")
                raise HTTPException(status_code=500, detail="Error fetching returns")


@app.get("/returns/{date}/{tickers}",status_code=status.HTTP_200_OK)
@cache(expire=CACHE_EXPIRATION,key_builder=custom_key_builder) 
async def get_returns_cross_section(date: str, tickers: str):
    date = parse_date(date)
    ticker_list = parse_tickers(tickers)
    query = """
    SELECT date, ticker, ret_1d
    FROM price_volume
    WHERE ticker = :ticker AND date = :date
    """
    data = []
    missing_tickers = []
    
    async with SessionLocal() as session:
        async with session.begin():  # Start a transaction
            try:
                for ticker in ticker_list:
                    result = await session.execute(text(query), {"ticker": ticker, "date": date})
                    query_result = result.fetchall()
                    
                    if query_result:
                        row = query_result[0]
                        data.append({"date": row[0].strftime('%Y-%m-%d'), "ticker": row[1], "ret_1d": float(row[2])})
                    else:
                        missing_tickers.append(ticker)
                        
                if not data:
                    raise HTTPException(status_code=404, detail=f"No data found for any tickers on {date}")

                result = {"data": data}
                if missing_tickers:
                    result['missing_tickers'] = missing_tickers
                return result

            except SQLAlchemyError as e:
                logging.error(f"Database error: {e}")
                raise HTTPException(status_code=500, detail="Error fetching returns")
        
    
if __name__ == "__main__":
    # test locally
    import uvicorn
    import subprocess
    
    # Start FastAPI app
    subprocess.Popen(['uvicorn', 'APIService.main:app', '--reload', '--host', '0.0.0.0', '--port', '8000','workers','--10'])
    # uvicorn.run("APIService.main:app", host="0.0.0.0", port=8000, workers=10)

    # --users: 500–2,000 users
    # --spawn-rate: 20–50 users/second
    # os.system('locust -f ./APIService/test/locustfile.py  --users 500 --spawn-rate 20 --host http://localhost:8000')
    subprocess.run(['locust', '-f', 'APIService/test/locust.py', '--host', 'http://localhost:8000'])

