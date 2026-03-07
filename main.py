"""
Entry point for Tushare data loading.
Run from project root: python main.py
"""
from logger import get_logger
import loader

if __name__ == "__main__":
    logger = get_logger(name="stock_loader", filename="stock", log_to_console=False)
    _ = loader.stock.StockBasic(logger)
    _ = loader.stock.StockDaily(logger, start='2026-02-01')



