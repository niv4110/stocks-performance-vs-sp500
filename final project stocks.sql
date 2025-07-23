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

-- 1.1 Stocks metadata table
CREATE TABLE stocks (
  id INT PRIMARY KEY AUTO_INCREMENT,
  Ticker VARCHAR(100),
  name VARCHAR(100),
  sector VARCHAR(100),
  industry VARCHAR(100),
  headquarters VARCHAR(100)
);

-- 1.2 Daily stock prices table
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

-- 1.3 S&P 500 index table
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

-- 2.1 Daily return for S&P 500
CREATE VIEW sp500_daily_returns AS
SELECT 
  trade_date,
  close_price,
  ROUND((close_price - LAG(close_price) OVER (ORDER BY trade_date)) / 
        LAG(close_price) OVER (ORDER BY trade_date) * 100, 4) AS daily_return_pct
FROM sp500_daily;

-- 2.2 Daily return per stock
CREATE VIEW stocks_daily_returns AS
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
-- 3. CUMULATIVE RETURNS (CORRECTED VERSION)
-- =============================================

-- 3.1 Cumulative returns per stock (accurate first vs. last price)
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
WHERE rn = 1
ORDER BY total_return_pct DESC;


-- 3.2 Cumulative return for S&P 500
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
-- 4. VOLATILITY & SHARPE RATIO
-- =============================================

-- 4.1 Sharpe Ratio for S&P 500
CREATE OR REPLACE VIEW sp500_sharpe_ratio AS
SELECT 
  'S&P 500' AS Ticker,
  ROUND(((AVG(daily_return_pct) * 252) - (0.02 * 100)) / 
        (STDDEV_POP(daily_return_pct) * SQRT(252)), 4) AS annual_sharpe_ratio
FROM sp500_daily_returns
WHERE daily_return_pct IS NOT NULL;

-- 4.2 Sharpe Ratio per stock
CREATE OR REPLACE VIEW stocks_sharpe_ratio AS
WITH daily_returns AS (
  SELECT 
    sp.stock_id, s.Ticker, sp.trade_date,
    (sp.close_price - LAG(sp.close_price) OVER (PARTITION BY sp.stock_id ORDER BY sp.trade_date)) /
    LAG(sp.close_price) OVER (PARTITION BY sp.stock_id ORDER BY sp.trade_date) * 100 AS daily_return_pct
  FROM stock_prices sp
  JOIN stocks s ON sp.stock_id = s.id
),
aggregated AS (
  SELECT 
    Ticker, stock_id,
    ROUND(AVG(daily_return_pct), 4) AS avg_daily_return,
    ROUND(STDDEV_POP(daily_return_pct), 4) AS stddev_daily_return
  FROM daily_returns
  WHERE daily_return_pct IS NOT NULL
  GROUP BY Ticker, stock_id
)
SELECT *,
  ROUND(((avg_daily_return * 252) - 2) / (stddev_daily_return * SQRT(252)), 4) AS annual_sharpe_ratio
FROM aggregated;

-- =============================================
-- 5. COMPARISONS
-- =============================================

-- 5.1 Compare Sharpe Ratios
CREATE OR REPLACE VIEW sharpe_ratio_comparison AS
SELECT stock_id, Ticker, annual_sharpe_ratio
FROM (
  SELECT stock_id, Ticker, annual_sharpe_ratio FROM stocks_sharpe_ratio
  UNION ALL
  SELECT NULL, Ticker, annual_sharpe_ratio FROM sp500_sharpe_ratio
) AS combined
ORDER BY annual_sharpe_ratio DESC;


-- 5.2 Compare Total Returns
CREATE OR REPLACE VIEW total_returns_comparison AS
SELECT stock_id, Ticker, total_return_pct FROM stocks_cumulative_returns
UNION ALL
SELECT NULL, 'S&P 500', total_return_pct FROM sp500_cumulative_return
ORDER BY total_return_pct DESC;

