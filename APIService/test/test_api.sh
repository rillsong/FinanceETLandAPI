#!/bin/bash

# Define base URL
BASE_URL="http://localhost:8000"

# locust test
locust -H $BASE_URL -u 10000 -r 10 --run-time 1h --headless

# Manual test
# Test /tickers endpoint
echo "Testing /tickers endpoint..."
curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/tickers" > result_tickers.txt
status_code=$(cat result_tickers.txt)

if [ "$status_code" -eq 200 ]; then
  echo "Success: /tickers returned status $status_code"
else
  echo "Error: /tickers returned status $status_code"
fi

echo "-----------------------------------"

# Test /returns/{ticker}/{start_date}/{end_date} endpoint
echo "Testing /returns/{ticker}/{start_date}/{end_date} endpoint..."
TICKER="AAPL"
START_DATE="2020-01-05"
END_DATE="2020-02-01"
curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/returns/$TICKER/$START_DATE/$END_DATE" > result_returns.txt
status_code=$(cat result_returns.txt)

if [ "$status_code" -eq 200 ]; then
  echo "Success: /returns/$TICKER/$START_DATE/$END_DATE returned status $status_code"
else
  echo "Error: /returns/$TICKER/$START_DATE/$END_DATE returned status $status_code"
fi

echo "-----------------------------------"

# Test /returns/{date}/{tickers} endpoint
echo "Testing /returns/{date}/{tickers} endpoint..."
DATE="2020-01-10"
TICKERS="AAPL,GOOGL,MSFT"
curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/returns/$DATE/$TICKERS" > result_returns_cross_section.txt
status_code=$(cat result_returns_cross_section.txt)

if [ "$status_code" -eq 200 ]; then
  echo "Success: /returns/$DATE/$TICKERS returned status $status_code"
else
  echo "Error: /returns/$DATE/$TICKERS returned status $status_code"
fi

echo "-----------------------------------"
