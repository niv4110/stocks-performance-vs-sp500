
-- =============================================
-- 📁 SQL Project: Stocks Performance vs S&P 500
-- 🔍 Objective: Analyze stocks performance, risk, and return metrics compared to the S&P 500 index
-- 🛠️ Tools: MySQL, Window Functions, Views
-- =============================================

-- 0. CREATE DATABASE
CREATE DATABASE stocks_project3;
USE stocks_project3;

-- =============================================
-- 1. TABLE CREATION
-- =============================================

CREATE TABLE stocks (
  id INT PRIMARY KEY AUTO_INCREMENT,
  Ticker VARCHAR(100),
  name VARCHAR(100),
  sector VARCHAR(100),
  industry VARCHAR(100),
  headquarters VARCHAR(100)
);

CREATE TABLE stock_prices (
  id INT AUTO_INCREMENT PRIMARY KEY,
  trade_date DATE NOT NULL,
  open_price DECIMAL(10,2) NOT NULL CHECK (open_price >= 0),
  high DECIMAL(10,2) NOT NULL CHECK (high >= 0),
  low DECIMAL(10,2) NOT NULL CHECK (low >= 0),
  close_price DECIMAL(10,2) NOT NULL CHECK (close_price >= 0),
  volume BIGINT NOT NULL CHECK (volume >= 0),
  stock_id INT NOT NULL,
  FOREIGN KEY (stock_id) REFERENCES stocks(id)
);

CREATE TABLE sp500_daily (
  id INT AUTO_INCREMENT PRIMARY KEY,
  trade_date DATE NOT NULL,
  open_price FLOAT,
  high_price FLOAT,
  low_price FLOAT,
  close_price FLOAT,
  volume BIGINT
);

-- =============================================
-- 2. DAILY RETURNS
-- =============================================

CREATE OR REPLACE VIEW sp500_daily_returns AS
SELECT 
  trade_date,
  close_price,
  ROUND((close_price - LAG(close_price) OVER (ORDER BY trade_date)) / 
        LAG(close_price) OVER (ORDER BY trade_date) * 100, 4) AS daily_return_pct
FROM sp500_daily;

CREATE OR REPLACE VIEW stocks_daily_returns AS
SELECT 
  sp.stock_id,
  s.Ticker,
  sp.trade_date,
  sp.close_price,
  ROUND((sp.close_price - LAG(sp.close_price) OVER (PARTITION BY sp.stock_id ORDER BY sp.trade_date)) / 
        LAG(sp.close_price) OVER (PARTITION BY sp.stock_id ORDER BY sp.trade_date) * 100, 4) AS daily_return_pct
FROM stock_prices sp
JOIN stocks s ON sp.stock_id = s.id;

-- =============================================
-- 3. CUMULATIVE RETURNS (FULL PERIOD)
-- =============================================

CREATE OR REPLACE VIEW stocks_cumulative_returns AS
SELECT stock_id, Ticker, total_return_pct
FROM (
  SELECT
    sp.stock_id,
    s.Ticker,
    ROUND((
      LAST_VALUE(sp.close_price) OVER (
        PARTITION BY sp.stock_id ORDER BY sp.trade_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) -
      FIRST_VALUE(sp.close_price) OVER (
        PARTITION BY sp.stock_id ORDER BY sp.trade_date
      )
    ) /
    FIRST_VALUE(sp.close_price) OVER (
      PARTITION BY sp.stock_id ORDER BY sp.trade_date
    ) * 100, 2) AS total_return_pct,
    ROW_NUMBER() OVER (PARTITION BY sp.stock_id ORDER BY sp.trade_date DESC) AS rn
  FROM stock_prices sp
  JOIN stocks s ON sp.stock_id = s.id
) final
WHERE rn = 1;

CREATE OR REPLACE VIEW sp500_cumulative_return AS
SELECT
  ROUND((
    LAST_VALUE(close_price) OVER (ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) -
    FIRST_VALUE(close_price) OVER (ORDER BY trade_date)
  ) /
  FIRST_VALUE(close_price) OVER (ORDER BY trade_date) * 100, 2) AS total_return_pct,
  'S&P 500' AS Ticker
FROM sp500_daily
LIMIT 1;

-- =============================================
-- 4. SHARPE RATIO (FULL PERIOD)
-- =============================================

CREATE OR REPLACE VIEW stocks_sharpe_ratio AS
WITH daily_returns AS (
  SELECT 
    sp.stock_id,
    s.Ticker,
    ((sp.close_price - LAG(sp.close_price) OVER (PARTITION BY sp.stock_id ORDER BY sp.trade_date)) /
     LAG(sp.close_price) OVER (PARTITION BY sp.stock_id ORDER BY sp.trade_date)) * 100 AS daily_return_pct
  FROM stock_prices sp
  JOIN stocks s ON sp.stock_id = s.id
),
aggregated AS (
  SELECT 
    stock_id,
    Ticker,
    COUNT(*) AS trading_days,
    ROUND(AVG(daily_return_pct), 4) AS avg_daily_return_pct,
    ROUND(STDDEV_POP(daily_return_pct), 4) AS stddev_daily_return_pct
  FROM daily_returns
  WHERE daily_return_pct IS NOT NULL
  GROUP BY stock_id, Ticker
)
SELECT 
  stock_id,
  Ticker,
  avg_daily_return_pct,
  stddev_daily_return_pct,
  ROUND(((avg_daily_return_pct * trading_days) - (0.02 * 100 * (trading_days / 252))) /
        (stddev_daily_return_pct * SQRT(trading_days)), 4) AS sharpe_ratio_full_period
FROM aggregated;

CREATE OR REPLACE VIEW sp500_sharpe_ratio AS
WITH daily_returns AS (
  SELECT 
    ((close_price - LAG(close_price) OVER (ORDER BY trade_date)) /
     LAG(close_price) OVER (ORDER BY trade_date)) * 100 AS daily_return_pct
  FROM sp500_daily
),
aggregated AS (
  SELECT 
    COUNT(*) AS trading_days,
    ROUND(AVG(daily_return_pct), 4) AS avg_daily_return_pct,
    ROUND(STDDEV_POP(daily_return_pct), 4) AS stddev_daily_return_pct
  FROM daily_returns
  WHERE daily_return_pct IS NOT NULL
)
SELECT 
  'S&P 500' AS Ticker,
  avg_daily_return_pct,
  stddev_daily_return_pct,
  ROUND(((avg_daily_return_pct * trading_days) - (0.02 * 100 * (trading_days / 252))) /
        (stddev_daily_return_pct * SQRT(trading_days)), 4) AS sharpe_ratio_full_period
FROM aggregated;

-- =============================================
-- 5. COMPARISONS
-- =============================================

CREATE OR REPLACE VIEW sharpe_ratio_comparison AS
SELECT stock_id, Ticker, sharpe_ratio_full_period
FROM (
  SELECT stock_id, Ticker, sharpe_ratio_full_period FROM stocks_sharpe_ratio
  UNION ALL
  SELECT NULL, Ticker, sharpe_ratio_full_period FROM sp500_sharpe_ratio
) combined
ORDER BY sharpe_ratio_full_period DESC;

CREATE OR REPLACE VIEW total_returns_comparison AS
SELECT stock_id, Ticker, total_return_pct FROM stocks_cumulative_returns
UNION ALL
SELECT NULL, 'S&P 500', total_return_pct FROM sp500_cumulative_return
ORDER BY total_return_pct DESC;
