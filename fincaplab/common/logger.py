import logging
import sys
from pathlib import Path


def setup_logger(
    name: str = "FinCapLab"
) -> logging.Logger:
    """
    为 FinCapLab 配置统一的日志记录器
    """
    logger = logging.getLogger(name)
    
    # 防止重复添加 Handler（pytest 或多次调用时常见问题）
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.DEBUG)

    # 1. 格式定义：包含来源模块、级别、具体信息等
    formatter = logging.Formatter(
        '%(name)s: [%(levelname)s] %(filename)s:%(lineno)d - %(funcName)s:  '
        '%(message)s | %(asctime)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 2. 控制台 Handler (输出到终端)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(logging.INFO) # 终端通常只看 INFO 以上

    # 3. 文件 Handler (保存到 logs/ 目录下)
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_dir / "app.log", encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG) # 文件里记录最详细的 DEBUG 信息

    # 4. 组装
    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

    return logger

# 创建一个全局默认实例，方便直接 import 使用
log = setup_logger()