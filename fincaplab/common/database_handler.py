from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Any, Generator, List, Optional

import pandas as pd

from fincaplab.common.logger import log

DB_DEFAULT_PATH: str = "data/finsrc.db"



def init_db(db_path: str = DB_DEFAULT_PATH) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS security_daily (
        product TEXT,
        code TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        status_code INTEGER DEFAULT 0,
        remark TEXT DEFAULT '',
        PRIMARY KEY (product, code, date)
    );
    """

    with get_db_connection(db_path) as conn:
        conn.execute(sql)

@contextmanager
def get_db_connection(
    db_path: str = DB_DEFAULT_PATH
) -> Generator[sqlite3.Connection, None, None]:
    """Provides a transactional scope around a series of database operations.

    Args:
        db_path (str): Filesystem path to the SQLite database file.

    Yields:
        sqlite3.Connection: An active SQLite connection with WAL mode.

    Raises:
        sqlite3.Error: Re-raises the error after logging it via custom logger.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        log.error(f"Database Transaction Failed: {str(e)}")
        raise e  # 抛出异常供上层业务逻辑（如 collector）判断是否需要中断
    except Exception as e:
        conn.rollback()
        log.error(f"Unexpected Error during DB operation: {str(e)}")
        raise e
    finally:
        conn.close()



def run_query(
    sql: str,
    params: Optional[List[Any]] = None,
    db_path: str = DB_DEFAULT_PATH
) -> pd.DataFrame:
    """
    Executes a SQL query and returns the result as a pandas DataFrame.
    """
    try:
        with get_db_connection(db_path) as conn:
            return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()



def run_execute(
    sql: str,
    params: Optional[List[Any]] = None,
    db_path: str = DB_DEFAULT_PATH
) -> None:
    """
    Executes a non-query SQL statement (INSERT, UPDATE, DELETE).
    """
    with get_db_connection(db_path) as conn:
        conn.execute(sql, params or [])