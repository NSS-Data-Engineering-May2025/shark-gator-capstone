import pytest
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.get_gator_attacks import get_gator_attacks

def test_fetch_api_data_returns_none_on_bad_endpoint():
    result = get_gator_attacks.fetch_api_data("http://invalid-endpoint")
    assert result is None

def test_load_to_minio_handles_empty_data():
    # Should not raise an exception with empty data
    try:
        get_gator_attacks.load_to_minio("")
    except Exception:
        pytest.fail("load_to_minio raised an exception with empty data")