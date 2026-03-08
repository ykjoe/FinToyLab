import akshare as ak
import pandas as pd
import time
from common.logger import log

def fetch_stock_data(symbol="002463", period="daily"):
    """
    抓取指定股票的行情信号。
    002463: 沪电股份 (PCB 行业标杆)
    """
    log.info(f"正在抓取指定股票的行情信号: {symbol}, {period}...")
    try:
        # 抓取数据
        df = ak.stock_zh_a_hist(symbol=symbol, period=period, adjust="qfq")
        
        if df.empty:
            log.error(f"信号丢失: {symbol} 未返回数据")
            return None

        return df
        
    except Exception as e:
        log.error("Collector Fail. %s", e)
        return None
