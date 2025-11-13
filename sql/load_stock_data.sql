TRUNCATE TABLE stock_prices_all;

-- Nạp dữ liệu từ CSV
COPY stock_prices_all(
    date,
    open,
    high,
    low,
    close,
    volume,
    symbol,
    industry,
    daily_return,
    price_range,
    volume_sma_5,
    sma_5,
    sma_20,
    ema_12,
    ema_26,
    macd,
    rsi_14
)
FROM '/opt/airflow/data/output/cleaned_stock.csv'
DELIMITER ','
CSV HEADER;