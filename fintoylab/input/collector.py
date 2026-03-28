from __future__ import annotations

from typing import Any

import pandas as pd

# Using relative imports for package portability
from fintoylab.common.database_handler import get_db_connection, run_query
from fintoylab.common.logger import log


class DailyDataCollector:
    """Orchestrates the synchronization of market data between sources and storage.

    This class implements the business logic for 'Smart Upserts', which identifies 
    new records to insert and existing records that require revision updates.

    Attributes:
        table_name (str): The target database table for storage.
    """


    def __init__(self, table_name: str = "security_daily"):
        """Initializes the collector with a specific table.

        Args:
            table_name (str): Name of the SQL table. Defaults to 'security_daily'.
        """
        self.table_name = table_name


    def sync_to_db(
            self,
            df: pd.DataFrame,
            product_type: str
            ) -> None:
        """Synchronizes a DataFrame to the database with revision detection.

        Args:
            df (pd.DataFrame): The standardized DataFrame containing market data.
            product_type (str): Category identifier (e.g., 'stock', 'futures').
        """
        if df.empty:
            log.info("Received empty DataFrame. Skipping synchronization.")
            return

        # Step 1: Fetch existing keys and close prices to detect revisions
        ## We use a combined key of (code, date) for identification
        query_sql = f"SELECT code, date, close FROM {self.table_name} WHERE product=?"
        existing_df = run_query(query_sql, params=[product_type])
        
        ## Map: (code, date) -> close_price
        if not existing_df.empty:
            keys = zip(existing_df['code'], existing_df['date'], strict=True)
            lookup = dict(zip(keys, existing_df['close'], strict=True))
        else:
            lookup = {}

        # Step 2: Iterate and determine strategy (Insert vs Update)
        ## Wrapping the loop in a single connection context for atomicity and speed
        with get_db_connection() as conn:
            for _, row in df.iterrows():
                key = (str(row["code"]), str(row["date"]))
                new_close = float(row["close"])

                if key in lookup:
                    ### Check if the closing price has changed (Data Revision)
                    if new_close != float(lookup[key]):
                        log.info(
                            f"Revision detected for {key[0]}@{key[1]}:"
                            f"{lookup[key]} -> {new_close}")
                        self._update_row(conn, row, product_type)
                else:
                    ### New record
                    self._insert_row(conn, row, product_type)


    def _insert_row(
            self,
            conn: Any,
            row: pd.Series,
            product_type: str
            ) -> None:
        """Executes a standard SQL INSERT for new market records."""
        sql = f"""
            INSERT INTO {self.table_name} 
            (product, code, date, open, high, low, close, volume, status_code, remark)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        data = (
            product_type, str(row['code']), str(row['date']),
            row['open'], row['high'], row['low'], row['close'],
            row['volume'], row.get('status_code', 0), row.get('remark', '')
        )
        conn.execute(sql, data)


    def _update_row(
            self,
            conn: Any,
            row: pd.Series,
            product_type: str
            ) -> None:
        """Executes a SQL UPDATE for corrected/revised market records."""
        sql = f"""
            UPDATE {self.table_name}
            SET open=?, high=?, low=?, close=?, volume=?, status_code=?, remark=?
            WHERE product=? AND code=? AND date=?
        """
        data = (
            row['open'], row['high'], row['low'], row['close'], row['volume'],
            3, "Auto-detected data revision",  # Status 3 indicates Revision
            product_type, str(row['code']), str(row['date'])
        )
        conn.execute(sql, data)