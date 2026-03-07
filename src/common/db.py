import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/fincap.db')

def get_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    # Example table for financial data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_data (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            date TEXT,
            price REAL,
            volume INTEGER
        )
    ''')
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
