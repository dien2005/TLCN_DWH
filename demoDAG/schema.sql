-- Chạy tự động khi container dwh-db khởi tạo lần đầu (mount vào /docker-entrypoint-initdb.d/)
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.kbs_price_raw (
    id            BIGSERIAL PRIMARY KEY,
    symbol        VARCHAR(20)     NOT NULL,
    trade_date    DATE            NOT NULL,
    open_price    NUMERIC(18,2),
    high_price    NUMERIC(18,2),
    low_price     NUMERIC(18,2),
    close_price   NUMERIC(18,2),
    volume        BIGINT,
    source        VARCHAR(20)     NOT NULL DEFAULT 'KBS',
    _loaded_at    TIMESTAMPTZ     NOT NULL DEFAULT now(),
    UNIQUE (symbol, trade_date, source)
);

CREATE INDEX IF NOT EXISTS idx_kbs_price_raw_symbol_date
    ON staging.kbs_price_raw (symbol, trade_date);