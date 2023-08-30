# Fintech_Project

A callable bull/bear contract, or CBBC in short form, is a derivative financial instrument that provides investors with a leveraged investment in underlying assets, which can be a single stock, or an index. CBBC is usually issued by third parties, mostly investment banks, but neither by stock exchanges nor by asset owners. The number of cbbc may impact the performance of Hong Kong stock-market index.

Southbound capital flow is the capital which flows from Mainland China to Hong Kong. Since this capital is up to RMB250 billion, it could be a substantial influence to the Hong Kong stock market.

In this project, I collected CBBC and southbound data from Hong Kong Exchanges and Clearing Limited website by web scraping, and then perform data cleaning using Python as well as operated a cloud server on Heroku to automate data updates.
Apart from these, I also built an interactive dashboard for data visualization and statistical analysis which advantages the supervision of Hong Kong stock-market index/.
Finally, I designed a simple moving averages crossover strategy and a Bollinger Bands crossover strategy for the simulation of single stock trading based on the stock price.

## cbbc_data.py

CBBC data
1. list the date from start date to end date (today)
2. download zip files from HKEX website recursively by changing the date in URL
3. append the file and save the data in the file to dataframe without the useless info
4. choose the needed data from dataframe (bull, bear, trade date) and calculate the relative number of Futures
5. read the csv file 'FHSI_futu.csv' to calculate Open to Close change of each day and save to dataframe
6. save the dataframe as a csv file 'Raw_data.csv'

Southbound data
7. use request library to grab Southbound data from HKEX website and save the needed data into dataframe
8. download HIS from yfinance library and then calculate cumulative sum of net southbound capital and moving Average of net southbound capital in the dataframe
9. save the dataframe as csv file 'sthbd&hsi.csv'

Save into Heroku 
10. connect to the Heroku PostgreSQL server
11. check if there is a table named ‘rdata’.
If rdata table exists, open Raw_data.csv and insert the last row of the csv file into the rdata table.
Else (rdata table does NOT exist), open Raw_data.csv and create a rdata table, then save all rows of data of the csv file into rdata table.
12. do the above step once more for sdata table with the data of  'sthbd&hsi.csv' 

## dashboard.py

1. connect to the Heroku PostgreSQL server
2. get the data from database and save as 3 dataframes which use in different way
3. create a selectbox sidebar for changing pages
4. there are 3 different page (Heng Seng Index, Callable Bull/Bear Contracts and Southbound Capital vs HSI)

For Heng Seng Index page, it shows a Heng Seng Index Graph which can select the showing date (1 month, 6 months, year to date, 1 year, all)

For Callable Bull/Bear Contracts page, it shows the comparison between bull and bear contracts by a line chart which can select the showing date, and the correlation of bull and bear contracts

For Southbound Capital vs HSI page, it shows two line charts, one describes Heng Seng Index and Net Southbound Mean (SH), the another one describes Heng Seng Index and Net Southbound Mean (SZ). Both of graphs have the correlation of them below the graph.

## trade_strategy.ipynb

1. download the stock data with the time period (from the beginning of 2017 to the end of 2021 in my case) from yahoo finance
2. define the trading logic which is the index used in the strategy
3. define the parameters and trading rules which are the signals you want to notice so as to buy or sell the stock.
4. Run the backtest to check the performance of the strategy by graphs and statistics
5. optimize the strategy by changing the parameters and evaluate it by heatmap

## sfc_short.py

1. list the date from start date to end date (today) with a period of a week
2. download zip files from SFC website recursively by changing the date in URL
3. save the data of files into the dataframe and save it as a csv file 'sfc_short_weekly.csv'
4. connect to the Heroku PostgreSQL server
5. check if there is a table named ‘short’.
If short table exists, open sfc_short_weekly.csv and insert the last row of the csv file into the short table.
Else (short table does NOT exist), open sfc_short_weekly.csv and create a short table, then save all rows of data of the csv file into short table.
