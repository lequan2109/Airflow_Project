CREATE TABLE IF NOT EXISTS stock_prices_all (
    symbol VARCHAR(10),
    industry VARCHAR(50),
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    daily_return FLOAT,
    price_range FLOAT,
    volume_sma_5 FLOAT,
    sma_5 FLOAT,
    sma_20 FLOAT,

    -- ===== SỬA LỖI: Đổi thứ tự 4 cột dưới đây cho khớp với file Spark =====
    ema_12 FLOAT,
    ema_26 FLOAT,
    macd FLOAT,
    rsi_14 FLOAT
    -- ===================================================================
);