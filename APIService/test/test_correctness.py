import pytest
import requests

BASE_URL = "http://localhost:8000"  

@pytest.mark.parametrize(
    "endpoint,params,expected_status",
    [
        ("/tickers", {}, 200),
        ("/returns/NVDA/2023-01-01/2023-01-31", {}, 200),
        ("/returns/2023-01-01/AAPL,MSFT", {}, 200),
        ("/returns/SOME_INVALID_TICKER/2023-01-01/2023-01-31", {}, 404),
    ],
)

def test_correctness(endpoint, params, expected_status):
    url = f"{BASE_URL}{endpoint}"
    response = requests.get(url, params=params)
    assert response.status_code == expected_status
    if expected_status == 200:
        assert "data" in response.json()

