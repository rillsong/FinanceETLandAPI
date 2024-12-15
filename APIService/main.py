from fastapi import FastAPI, HTTPException, status,Query, Depends
from fastapi.responses import JSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from contextlib import asynccontextmanager
from prometheus_fastapi_instrumentator import Instrumentator
import redis.asyncio as redis
import os
import logging
import pandas as pd
from datetime import datetime
import re

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app_debug.log"), logging.StreamHandler()],
)

# DB should be read-repeatable
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

# Creating DB connection
engine = create_engine (DATABASE_URL,pool_size = 10, max_overflow = 20)

#==================Utility===================
async def clear_cache():
    """Clear the Redis cache."""
    await FastAPICache.clear()
    logging.info("Cache cleared.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Redis cache backend
    redis_client = redis.from_url(REDIS_URL)
    FastAPICache.init(RedisBackend(redis_client), prefix="fastapi-cache")
    logging.info("Cache initialized")

    yield # running state
    
    # On shutdown, clear the cache
    await clear_cache()
    await redis_client.close()
    logging.info("Cache closed")


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

# Build API
app = FastAPI(lifespan=lifespan, title = "data_API",debug = True)
app.state.limiter = limiter # rate limiter
instrumentator.instrument(app).expose(app) # performance metrics  
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
@cache(expire=600)  # Cache for 10 minutes
async def get_tickers():
    """Retrieve a list of all unique tickers from the price_volume table."""
    query = "SELECT DISTINCT ticker FROM price_volume"
    try:
        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text(query)).fetchall()
        if not result: raise HTTPException(status_code=404, detail="No data found")
        tickers = [row[0] for row in result]
        return JSONResponse(content={"tickers": tickers}, status_code=200)
    
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching tickers")


@app.get("/returns/{ticker}/{start_date}/{end_date}",status_code=status.HTTP_200_OK)
@cache(expire=300)  # Cache for 5 minutes
async def get_returns(ticker: str, start_date: str, end_date: str):
    """Calculate and return daily returns for a ticker within a date range."""

    start_date = parse_date(start_date)
    end_date = parse_date(end_date)
    ticker_list = parse_tickers(ticker)
    if len(ticker_list)>1: raise HTTPException(status_code=400,detail="Only provide one ticker name.")
    
    try:
        query = """
        SELECT date, ticker, ret_1d
        FROM price_volume
        WHERE ticker = :ticker
        AND date BETWEEN :start_date AND :end_date
        """
        
        # Execute the query and fetch the results
        with engine.connect() as conn:
            with conn.begin():
                query_result = conn.execute(text(query), {"ticker": ticker, "start_date": start_date, "end_date": end_date}).fetchall()
        
        if not query_result: raise HTTPException(status_code=404, detail="No data found for the given ticker and date range")

        data = [{"date": row[0].strftime('%Y-%m-%d'), "ticker": row[1], "ret_1d": float(row[2])} for row in query_result]
        return JSONResponse(content={"data": data}, status_code=200)
    
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching returns")


@app.get("/returns/{date}/{tickers}",status_code=status.HTTP_200_OK)
@cache(expire=300)  # Cache for 5 minutes
async def get_returns_cross_section(date: str, tickers: str):
    """
    Get cross-sectional returns for multiple tickers on a specific date.
    """

    # Validate the date format to ensure it is in 'YYYY-MM-DD'
    date = parse_date(date)
    ticker_list = parse_tickers(tickers)
    query = """
    SELECT date, ticker, ret_1d
    FROM price_volume
    WHERE ticker = :ticker AND date = :date
    """
    data = []
    missing_tickers = []
    try:
        with engine.connect() as conn:
            with conn.begin():
                for ticker in ticker_list:
                    query_result = conn.execute(text(query), {"ticker": ticker, "date": date}).fetchall()
                    if query_result:
                        row = query_result[0]
                        data.append({"date": row[0].strftime('%Y-%m-%d'), "ticker": row[1], "ret_1d": float(row[2])})
                    else: 
                        missing_tickers.append(ticker)

        if not data: raise HTTPException(status_code=404, detail=f"No data found for any tickers on {date}")
        result = {"data":data}
        if missing_tickers: result['missing_tickers'] = missing_tickers
        return JSONResponse(content=result, status_code=200)
    
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching returns")    
    
if __name__ == "__main__":
    # test locally
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=10)

