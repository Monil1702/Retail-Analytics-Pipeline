CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.retail_transactions (
    transaction_id VARCHAR(40) PRIMARY KEY,
    customer_id VARCHAR(40) NOT NULL,
    product_id VARCHAR(40) NOT NULL,
    transaction_ts TIMESTAMPTZ NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    channel VARCHAR(20) NOT NULL,
    region VARCHAR(30) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    sales_amount NUMERIC(14, 2) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_retail_transaction_ts
    ON analytics.retail_transactions (transaction_ts);
CREATE INDEX IF NOT EXISTS idx_retail_region_channel
    ON analytics.retail_transactions (region, channel);
