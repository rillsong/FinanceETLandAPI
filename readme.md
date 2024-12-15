## 1. Approach and Methodology

### ETL Pipeline

**Downloading Data**: in `fetch_data`, we enabled yfinance's multi-threaded batch downloads to improve efficiency when fetching data for multiple tickers. Also we add a `RateLimiterSession` to conform to yfinance's rate limiting policy and prevents data corruption. After downloading, the data will be saved in a   `landing_folder` for further steps.

**Transforming Data**: in `transform_pipeline`, we validate each field data types against predefined schema, check data ranges, drop duplicated rows, drop missing values and calculate daily return. After processing, the data will be saved in a `processed_folder` for further steps.

**Loading Data**: we create a `price_volume` table in `MarketData.db` in MySQL to save the downloaded data. In ` upsert_parallel_by_ticker`, we use `concurrent.futures` to batch data by stock tickers and upserted into the table.

**Task Scheduling**: we use Apache Airflow to trigger ETL pipeline to ensure task dependency management and retry mechanisms. Also we configure Airflow with Celery as the task queue to handle distributed execution of tasks efficiently.

### API Development

**Framework Selection**: we use`FastAPI + uvicorn` framework for it supports asynchronous tasks with multiple workers, also relatively lightweight.

**Caching**: we integrate `fastapi_cache` with `redis.asyncio` as the caching backend to improve API response times by storing and retrieving frequently accessed data with expiration time.

**Performance**: we use prometheus to monitor API performance (localhost:8000/metrics).

### Deliverables

we use Docker to host Airflow ETL pipeline (localhost:8080), APIService (localhost:8000). Downloaded data is in the`MySql`container.

run 

`docker compose -f docker-compose.base.yaml up --build`

`docker compose -f docker-compose.airflow.yaml up --build`

`docker compose -f docker-compose.api.yaml up --build`

`docker compose -f docker-compose.prom.yaml up --build`

## 2. Assumptions

- Assume all SP500 stock tickers (`ticker.csv`) are provided for download exist in the yfinance. If we cannot get data of some tickers, the missing ones will be recorded in the `airflow/logs`.
- Assume data size is not too large, so we do not need pagination, or implement rate limiting to clients.

* Assume that cached data is suitable for most API queries, with occasional direct database queries for less frequent requests.

## 3. Challenges Faced and Solutions

### Data Read and Write Performance

We used database connection pooling and implemented multi-threaded batch insertion using `concurrent.futures` to parallelize the process. Data is partitioned by tickers. We also set  `uvicorn` multiple workers for API clients, and cached data with expiration policy to balance cache freshness and performance.

## 4. Self-assessment

- We can monitor the ETL and web service performance in airflow and prometheus.
- API response time: most queries completing in <50ms when tested locally; API scalibility: can handle 500 concurrent requests;
- ETL downloading: finish 1-year 500 tickers daily data download within minutes; ETL upserting: batch upsert of 1-year 500 tickers daily data within seconds.
