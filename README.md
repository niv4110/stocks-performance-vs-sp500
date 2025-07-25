
📊 Stocks Performance vs S&P 500 – SQL Analytics Project

📊 Key Metrics
Daily & Cumulative Returns

Volatility

Sharpe Ratio (with 2% risk-free rate)

Comparative Analysis to S&P 500

🧱 Database Structure
stocks: stock metadata

stock_prices: daily prices per stock

sp500_daily: daily index data

🔎 Analytical Views
stocks_daily_returns

stocks_cumulative_returns

stocks_sharpe_ratio

sp500_daily_returns

sp500_sharpe_ratio

sharpe_ratio_comparison

total_returns_comparison

📂 **Data Files**  
To replicate the analysis, import the following CSV files into your MySQL database:

- `stock_prices.csv` → table: `stock_prices`  
- `sp500_daily_data.csv` → table: `sp500_daily`  
- `Stocks_Table_Preview.csv` → table: `stocks`  

These files are located in the root directory of this repository.  
**Make sure to load the data _after_ creating the tables using the provided SQL schema file.**


🚀 Next Steps
Build a Tableau dashboard

🗓️ Timeline
SQL Project: ✅

Tableau Dashboard: ⏳

© 2025 Niv Bino | Project built with MySQL + Tableau

