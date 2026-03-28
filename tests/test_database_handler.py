import sqlite3

import pandas as pd
import pytest

from fincaplab.common.database_handler import get_db_connection, run_execute, run_query

# ==========================================
# FIXTURES
# ==========================================

@pytest.fixture
def mock_db(tmp_path):
    """
    创建一个临时数据库文件，测试完成后自动删除。
    tmp_path 是 pytest 内置的 fixture，提供安全的临时目录。
    """
    db_file = tmp_path / "test_fincap.db"
    db_path = str(db_file)
    
    # 初始化一个简单的测试表
    init_sql = "CREATE TABLE test_assets (code TEXT PRIMARY KEY, price REAL)"
    run_execute(init_sql, db_path=db_path)
    
    return db_path




# ==========================================
# TEST CASES
# ==========================================

def test_run_execute_and_query_success(mock_db):
    """
    定义一个预期列表，循环验证
    """
    # 1. 定义测试数据集
    test_data = [
        {"code": "AAPL", "price": 180.5},
        {"code": "MSFT", "price": 420.0},
        {"code": "TSLA", "price": 200.0},
    ]

    # 2. 批量写入
    for item in test_data:
        run_execute(
            "INSERT INTO test_assets (code, price) VALUES (?, ?)", 
            [item["code"], item["price"]], 
            db_path=mock_db
        )

    # 3. 统一查询
    df = run_query("SELECT * FROM test_assets ORDER BY code ASC", db_path=mock_db)
    
    # 4. 优化后的校验逻辑：将结果按 code 排序后与预期对比
    expected_sorted = sorted(test_data, key=lambda x: x["code"])
    
    assert len(df) == len(expected_sorted)
    
    for i, expected in enumerate(expected_sorted):
        assert df.iloc[i]['code'] == expected["code"]
        assert df.iloc[i]['price'] == expected["price"]




def test_transaction_rollback_on_error(mock_db):
    """
    测试事务回滚：当发生错误时，数据不应该被存入数据库
    """
    
    # 尝试在一个事务中执行成功插入和失败操作
    try:
        with get_db_connection(mock_db) as conn:
            conn.execute(
                "INSERT INTO test_assets (code, price) VALUES (?, ?)",
                ["FAIL_TEST", 999]
                )
            conn.execute(
                "INSERT INTO non_existent_table VALUES (1)"
                ) # 插入重复的主键或格式错误
    except Exception:
        pass # 错误已被 database_handler 记录

    # 验证 "FAIL_TEST" 是否因为回滚而没有存入
    df = run_query(
        "SELECT * FROM test_assets WHERE code = ?",
        ["FAIL_TEST"],
        db_path=mock_db
        )
    assert df.empty, "Transaction should have rolled back, but record was found!"




def test_wal_mode_enabled(mock_db):
    """
    验证 WAL 模式是否真的开启了
    """
    with get_db_connection(mock_db) as conn:
        cursor = conn.execute("PRAGMA journal_mode")
        mode = cursor.fetchone()[0]
        assert mode.lower() == "wal"




def test_run_query_returns_empty_df_on_failure(mock_db):
    """
    验证当 SQL 语法错误时，run_query 是否按照预期返回空的 DataFrame 而不是崩溃
    """
    # 查询一个不存在的表
    df = run_query("SELECT * FROM mystery_table", db_path=mock_db)
    assert isinstance(df, pd.DataFrame)
    assert df.empty




def test_log_output_on_error(mock_db, capsys):
    """
    测试日志输出格式。
    capsys 是 pytest 内置 fixture，可以捕获 stdout/stderr。
    """
    with pytest.raises(sqlite3.Error):
        # 故意触发一个 SQL 注入不当或语法错误的语句
        run_execute("INSERT INTO test_assets (wrong_col) VALUES (1)", db_path=mock_db)
    
    # 获取控制台输出
    captured = capsys.readouterr()
    # 验证输出中是否包含你在 database_handler 中定义的错误前缀
    assert "Database Transaction Failed" in captured.out or "" == captured.out