#!/usr/bin/env python3

import fintoylab.input.akshare_read as ak_read


def main(
    
) -> None:
    print("FinToyLab is running!")
    data = ak_read.fetch_stock_data()
    if data is not None:
        print(data[['日期', '收盘', '成交量']])

if __name__ == "__main__":
    main()