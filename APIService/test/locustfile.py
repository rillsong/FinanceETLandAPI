"""simulate multiple users hitting endpoints"""

from locust import HttpUser, task, between
class APIPerformanceTest(HttpUser):
    host = "http://localhost:8000"  # server base URL
    wait_time = between(1, 3)  # Time to wait between requests

    @task(1)
    def test_get_tickers(self):
        self.client.get("/tickers")

    @task(2)
    def test_get_returns(self):
        self.client.get("/returns/MSFT/2020-02-01/2021-02-01")

    @task(1)
    def test_get_cross_section_returns(self):
        self.client.get("/returns/2021-01-01/AAPL,MSFT,NVDA")
