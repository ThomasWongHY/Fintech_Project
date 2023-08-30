import requests 
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import psycopg2
import csv
import pytz
tz = pytz.timezone('Asia/Hong_Kong')
from bs4 import BeautifulSoup as bs
import demjson
import numpy as np
from datetime import datetime, timedelta
import yfinance as yf

# Input url and downlaod the .zip file to the folder
# From link: https://www.hkex.com.hk/eng/cbbc/download/dnCSV.asp
# Link for Current Month: https://www.hkex.com.hk/eng/cbbc/download/CBBC11.zip

def download_zip_from_hkex(url, save_name, chunk_size=128):
    r = requests.get(url, stream=True)
    # Write the file as .zip and save as {save_name}
    with open(save_name, 'wb') as output:
        for chunk in r.iter_content(chunk_size=chunk_size):
            output.write(chunk)

now_dt=dt.datetime.now(tz)
end_str=now_dt.strftime("%Y-%m-%d")
start_dt=now_dt-timedelta(days=365)   # HKex database on web storged data for about last 200 days
start_str=start_dt.strftime("%Y-%m-%d")
datelist=pd.date_range(start=start_str ,end=end_str, freq='M')
datelist=[d.strftime("%Y%m") for d in datelist]

for month in datelist:
    print('Downloading', month)
    download_zip_from_hkex(f'https://www.hkex.com.hk/eng/cbbc/download/CBBC{month[-2:]}.zip', f'CBBC_{month}.zip', chunk_size=128)

cbbc_full=pd.DataFrame()

for month in datelist:
    print('Appending ', month)
    raw=pd.read_csv(f'CBBC_{month}.zip', compression='zip', header=0, sep='\t', encoding='utf-16')
    raw=raw[:-3]
    cbbc_full=cbbc_full.append(raw)

cbbc_full['Bull/Bear']=cbbc_full['Bull/Bear'].str.strip()
# Filter out CBBC that expired
cbbc_full=cbbc_full[cbbc_full['Last Trading Date']!=cbbc_full['Trade Date']]
# Turn date to datetime format
cbbc_full['Trade Date']=cbbc_full['Trade Date'].astype('datetime64[ns]')
# Calucate the relative number of Futures
cbbc_full['future']=(cbbc_full['No. of CBBC still out in market *'])/cbbc_full['Ent. Ratio^']/100*2
# Pivot table of the figures
cbbc_full=cbbc_full.groupby(['Underlying','Trade Date','Bull/Bear'])['future'].sum()['HSI'].to_frame()
df=pd.pivot_table(cbbc_full, values=['future'], index=['Trade Date'],columns=['Bull/Bear'])['future']

future=pd.read_csv('FHSI_futu.csv',index_col=0)
# Covert date as datetime
future['Trade Date']=future['time_key'].astype('datetime64[ns]')
future.set_index('Trade Date', inplace=True)
# Calculate Open to Close change of each day
future['o2c_change']=future['close']/future['open']-1

df['f_o2c_change']=future['o2c_change']

# save data as csv
df.to_csv('Raw_data.csv')



now_dt=dt.datetime.now(tz)
end_str=now_dt.strftime("%Y-%m-%d")
start_dt=now_dt-timedelta(days=210)   # HKex database on web storged data for about last 200 days
start_str=start_dt.strftime("%Y-%m-%d")

# Create date list as the format of the link required (YYYYMMDD)
datelist=pd.date_range(start=start_str, end=end_str)
datelist=[datetime.strftime(date, '%Y%m%d') for date in datelist]

# Default values
market_idx=0
summary={} 

for date in datelist:
    link=f'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{date}c.js?_=1611069994631'
    r=requests.get(link, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36'}, stream=True)
    soup=bs(r.text, 'html.parser')
    try:
        soup1=soup.text.replace('tabData = ','')
        raw=demjson.decode(soup1)
    except:
        continue
    
    for market_idx in range(0,len(raw)):
        tradingday=raw[market_idx]['tradingDay']
        if tradingday!=1:
            continue
        market=raw[market_idx]['market']
        date=raw[market_idx]['date']
        summary_table=raw[market_idx]['content'][0]['table']
        summary_col=summary_table['schema'][0]
        summary_col=['Market','Date']+summary_col
        summary_data=[float(summary_table['tr'][i]['td'][0][0].replace(',','')) for i in range(0,len(summary_table['tr']))]
        summary_data=[market,date]+summary_data
        summary_df=pd.DataFrame.from_dict([dict(zip(summary_col,summary_data))])
        try:
            summary[market]
            summary[market]=summary[market].append(summary_df, ignore_index=True)
        except:
            summary[market]=summary_df


# Save the dict
np.save('southbound.npy', summary)

# Set index as Date for plotting
summary['SSE Southbound'].set_index('Date', inplace=True)
summary['SZSE Southbound'].set_index('Date', inplace=True)

# Download HSI data online
hsi=yf.download('^HSI')
hsi.index=hsi.index.strftime('%Y-%m-%d')
df=pd.DataFrame()
# Calculate net southbound capital flow to HK market
df['net_sse']=summary['SSE Southbound']['Buy Turnover']-summary['SSE Southbound']['Sell Turnover']
df['net_szse']=summary['SZSE Southbound']['Buy Turnover']-summary['SSE Southbound']['Sell Turnover']
df['net_southbound']=df['net_sse']+df['net_szse']
df['net_southbound_cum']=df['net_southbound'].cumsum()    # Cumulative sum of net southbound capital
df['net_southbound_mean']=df['net_southbound'].rolling(3).mean()    # Moving Average of net southbound capital
df['hsi']=hsi['Adj Close']

df.to_csv('sthbd&hsi.csv')

def create_rtables():
    #create tables in the PostgreSQL database
    commands = (
        """
        CREATE TABLE rdata (
            trade_date VARCHAR(30) NOT NULL PRIMARY KEY,
            bear VARCHAR(30) NOT NULL,
            bull VARCHAR(30) NOT NULL,
            f_o2c_change VARCHAR(30)
        )
        """)
    # connect to the PostgreSQL server
    conn=psycopg2.connect(
        user="dnddsxgwtueevh",
        password="c6384386c2e42ea38b53e30711401007ff2fd0f4bf3f29b8779e079293004f78",
        host="ec2-3-233-7-12.compute-1.amazonaws.com",
        database="dektg47b7r8tmp"
        )
    cur=conn.cursor()
    
    cur.execute(
        """
        SELECT * FROM information_schema.tables 
        WHERE  table_name='rdata'
        """)
    result=bool(cur.rowcount)
    
    if result: #rdata table exists
        print("Insert New Row in rTable")
        with open('Raw_data.csv', 'r') as f:
            reader = csv.reader(f)
            Rows=list(reader) 
            Tot_rows=len(Rows)-1
            lastrow=Rows[Tot_rows]
            cur.execute(
                "INSERT INTO rdata VALUES (%s, %s, %s, %s)",
                lastrow)

    else:#rdata table does NOT exist
        print("Create rTable")
        with open('Raw_data.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            #cur.execute("DROP TABLE IF EXISTS rdata")
            cur.execute(commands)
            for row in reader:
                cur.execute(
                "INSERT INTO rdata VALUES (%s, %s, %s, %s)",
                row
            )

    # commit the changes
    conn.commit()

def create_stables():
    #create tables in the PostgreSQL database
    commands = (
        """
        CREATE TABLE sdata (
            date VARCHAR(30) NOT NULL PRIMARY KEY,
            net_sse VARCHAR(30) NOT NULL,
            net_szse VARCHAR(30) NOT NULL,
            net_southbound VARCHAR(30) NOT NULL,
            net_southbound_cum VARCHAR(30) NOT NULL,
            net_southbound_mean VARCHAR(30),
            hsi VARCHAR(30) NOT NULL
        )
        """)
    # connect to the PostgreSQL server
    conn=psycopg2.connect(
        user="dnddsxgwtueevh",
        password="c6384386c2e42ea38b53e30711401007ff2fd0f4bf3f29b8779e079293004f78",
        host="ec2-3-233-7-12.compute-1.amazonaws.com",
        database="dektg47b7r8tmp"
        )
    cur=conn.cursor()

    cur.execute(
        """
        SELECT * FROM information_schema.tables 
        WHERE  table_name='sdata'
        """)
    result=bool(cur.rowcount)
    
    if result: #sdata table exists
        print("Insert New Row in sTable")
        with open('sthbd&hsi.csv', 'r') as f:
            reader = csv.reader(f)
            Rows=list(reader) 
            Tot_rows=len(Rows)-1
            lastrow=Rows[Tot_rows]
            cur.execute("INSERT INTO sdata VALUES (%s, %s, %s, %s, %s, %s, %s)",
                lastrow)

    else: #sdata table does NOT exist
        print("Create sTable")
        with open('sthbd&hsi.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            #cur.execute("DROP TABLE IF EXISTS sdata")
            cur.execute(commands)
            for row in reader:
                cur.execute(
                "INSERT INTO sdata VALUES (%s, %s, %s, %s, %s, %s, %s)",
                row
            )

    # commit the changes
    conn.commit()

#if __name__ == '__main__':
    #create_rtables()
    #create_stables()
