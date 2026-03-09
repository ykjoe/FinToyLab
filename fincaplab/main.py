#!/usr/bin/env python3

import input.collector as collect


def main(
    
) -> None:
    print("FinCapLab is running!")
    data = collect.fetch_stock_data()
    if data is not None:
        print(data[['日期', '收盘', '成交量']])

if __name__ == "__main__":
    main()