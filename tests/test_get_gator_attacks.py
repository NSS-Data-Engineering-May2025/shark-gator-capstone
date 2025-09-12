import pytest
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.get_gator_attacks import fetch_api_data, load_to_minio

def test_fetch_api_data_returns_none_on_bad_endpoint():
    result = fetch_api_data("http://invalid-endpoint")
    assert result is None

def test_load_to_minio_handles_empty_data():
    # Should not raise an exception with empty data
    try:
        load_to_minio("")
    except Exception:
        pytest.fail("load_to_minio raised an exception with empty data")