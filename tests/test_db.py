# Test for db module
import pytest
from src.common.db import init_db, get_connection

def test_init_db():
    init_db()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='financial_data'")
    assert cursor.fetchone() is not None
    conn.close()