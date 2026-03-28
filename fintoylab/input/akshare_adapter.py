from __future__ import annotations

from typing import Dict

import pandas as pd

# mapping
AKSHARE_COLUMN_MAP: Dict[str, str] = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
}




# addapter
class DataAdapter:
    """
    数据适配器：将外部 API 返回的 DataFrame 转换为 fincaplab 标准格式。
    """

    @staticmethod
    def align_akshare_data(
        df: pd.DataFrame,
        product_type: str,
        code: str
    ) -> pd.DataFrame:
        """
        对齐 AkShare 历史数据列名并注入元数据。
        """
        # 1. 重命名列
        df = df.rename(columns=AKSHARE_COLUMN_MAP)
        
        # 2. 注入缺失的标准字段
        df["product"] = product_type
        df["code"] = code
        
        # 3. 确保 status_code 和 remark 存在（如果原始数据没有）
        if "status_code" not in df.columns:
            df["status_code"] = 0
        if "remark" not in df.columns:
            df["remark"] = ""

        # 4. 只保留数据库 Schema 中定义的列，过滤掉多余的（如“振幅”等）
        # 这里可以直接引用 database_handler 里的 SECURITY_TABLE_SCHEMA.keys()
        return df