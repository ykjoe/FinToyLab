import pandas as pd
import pytest

from fincaplab.common.database_handler import run_execute, run_query
from fincaplab.input.collector import DailyDataCollector

# ==========================================
# FIXTURES
# ==========================================

@pytest.fixture
def db_with_schema(tmp_path):
    """Fixture to provide a clean database with the required schema."""
    db_path = str(tmp_path / "test_sync.db")
    # Manually create the table for testing
    run_execute("""
        CREATE TABLE security_daily (
            product TEXT, code TEXT, date TEXT, 
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            status_code INTEGER, remark TEXT,
            PRIMARY KEY (product, code, date)
        )
    """, db_path=db_path)
    return db_path




# ==========================================
# TEST CASES
# ==========================================

def test_sync_logic_insert_and_revision(db_with_schema, monkeypatch):
    """Tests that the collector correctly inserts and then updates revised data."""
    # 1. Mock the default DB path to our test DB
    monkeypatch.setattr(
        "fincaplab.common.database_handler.DB_DEFAULT_PATH", db_with_schema
        )
    
    collector = DailyDataCollector()
    
    # 2. Prepare initial data
    initial_data = pd.DataFrame([{
        "code": "000001", "date": "2023-01-01", 
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1000
    }])
    
    # First sync: Should INSERT
    collector.sync_to_db(initial_data, "stock")
    
    df_after_first = run_query("SELECT * FROM security_daily", db_path=db_with_schema)
    assert len(df_after_first) == 1
    assert df_after_first.iloc[0]['close'] == 10.5
    assert df_after_first.iloc[0]['status_code'] == 0

    # 3. Prepare revised data (Same key, different close price)
    revised_data = pd.DataFrame([{
        "code": "000001", "date": "2023-01-01", 
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.8, "volume": 1000
    }])
    
    # Second sync: Should UPDATE
    collector.sync_to_db(revised_data, "stock")
    
    df_after_second = run_query("SELECT * FROM security_daily", db_path=db_with_schema)
    assert len(df_after_second) == 1
    assert df_after_second.iloc[0]['close'] == 10.8
    assert df_after_second.iloc[0]['status_code'] == 3  # Check for revision status
    assert "revision" in df_after_second.iloc[0]['remark'].lower()