import pytest
import pandas as pd
from App import fetch_crypto_data, transform_data

def test_fetch_crypto_data():
    data = fetch_crypto_data()
    assert isinstance(data, pd.DataFrame)
    
    if not data.empty:
        # Check if the required columns are present in the data
        assert 'id' in data.columns
        assert 'name' in data.columns
        assert 'symbol' in data.columns
        assert 'quote' in data.columns

# Use some fake raw data
def test_transform_data():
    raw_data = pd.DataFrame([
        {
            "id": 1,
            "name": "Bitcoin",
            "symbol": "BTC",
            "slug": "bitcoin",
            "date_added": "2013-04-28T00:00:00Z",
            "quote": {"USD": {"price": 100000, "market_cap": 900000000, "market_cap_dominance": 60, "percent_change_1h": -1, "percent_change_24h": 1, "volume_change_24h": 1, "last_updated": "2025-01-05"}}
        }
    ])

    metadata, market_data = transform_data(raw_data)

# Test if transformation correctly created metadata DataFrame
    assert isinstance(metadata, pd.DataFrame)
    assert 'Id' in metadata.columns
    assert 'Name' in metadata.columns
    assert 'Symbol' in metadata.columns
    assert 'Slug' in metadata.columns
    assert 'DateAdded' in metadata.columns
# Test if transformation correctly created marketdata DataFrame
    assert isinstance(market_data, pd.DataFrame)
    assert 'CryptocurrencyId' in market_data.columns
    assert 'Price' in market_data.columns
    assert 'MarketCap' in market_data.columns
    assert 'MarketCapDominance' in market_data.columns
    assert 'PercentChange1H' in market_data.columns
    assert 'PercentChange24H' in market_data.columns
    assert 'VolumeChange24H' in market_data.columns
    assert 'LastUpdated' in market_data.columns
    assert 'RecordTimestamp' in market_data.columns
