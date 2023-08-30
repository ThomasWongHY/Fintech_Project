import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import psycopg2


def main():

    conn=psycopg2.connect(
        user="dnddsxgwtueevh",
        password="c6384386c2e42ea38b53e30711401007ff2fd0f4bf3f29b8779e079293004f78",
        host="ec2-3-233-7-12.compute-1.amazonaws.com",
        database="dektg47b7r8tmp"
        )
    df1 = pd.read_sql_query('select * from "rdata"',con=conn)       #get the data of CBBC
    df2 = pd.read_sql_query('select * from "sdata"',con=conn)       #get the data of Southbound
    df3 = pd.read_sql_query('select * from "mdata"',con=conn)       #get the data of Southbound mean
    
    df1[['bear', 'bull', 'f_o2c_change']] = df1[['bear', 'bull', 'f_o2c_change']].apply(pd.to_numeric)
    df2[['net_sse', 'net_szse', 'net_southbound', 'net_southbound_cum', 'net_southbound_mean', 'hsi']] = df2[['net_sse', 'net_szse', 'net_southbound', 'net_southbound_cum', 'net_southbound_mean', 'hsi']].apply(pd.to_numeric)
    df3[['net_sse', 'net_szse', 'net_sse_mean', 'net_szse_mean']] = df3[['net_sse', 'net_szse', 'net_sse_mean', 'net_szse_mean']].apply(pd.to_numeric)

    st.set_page_config(layout="wide")
    page = st.sidebar.selectbox("Choose a page", ['Heng Seng Index', 'Callable Bull/Bear Contracts', 'Southbound Capital vs HSI'])

    if page == 'Heng Seng Index':
        
        st.title('Heng Seng Index Graph')
        fig = px.line(df2, x='date', y='hsi', title='Time Series of Heng Seng Index')
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        fig.update_layout(width=1000,height=700)
        st.write(fig)
        #st.dataframe(df_hsi)
        

    elif page == 'Callable Bull/Bear Contracts':
        
        st.title('Comparison Between Bull and Bear Contracts')
        st.markdown('### Analysis')
        df_cbbc = df1[['trade_date', 'bear', 'bull']]
        
        x = df1[['bear', 'bull']]
        
        fig = px.line(df_cbbc, x='trade_date', y=df_cbbc.columns,
                      title='Time Series of Callable Bull/Bear Contracts')
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        fig.update_layout(width=1000,height=700)
        st.write(fig)
        st.write(x.corr(method='pearson'))
        #st.dataframe(df_cbbc)
        
    else:
        st.title('Southbound Capital vs HSI')
        df_sthbd = df2[['date', 'hsi', 'net_szse']]
        df_mean = df3[['date', 'net_sse_mean', 'net_szse_mean']]
        subfiga = make_subplots(specs=[[{"secondary_y": True}]])

        figa = px.line(df_sthbd, x='date', y=df_sthbd.filter(items=['hsi']).columns)
        fig2a = px.line(df_mean, x='date', y=df_mean.filter(items=['net_sse_mean']).columns)
        
        fig2a.update_traces(yaxis="y2")

        subfiga.add_traces(figa.data + fig2a.data)
        subfiga.layout.xaxis.title="Date"
        subfiga.update_yaxes(title_text="<b>Heng Seng Index</b>", secondary_y=False)
        subfiga.update_yaxes(title_text="<b>Net Southbound Mean (SH)</b><br>(HK$ million)", secondary_y=True)
        
        subfiga.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        df_sse=pd.DataFrame()
        df_sse[['hsi']] = df2[['hsi']]
        df_sse[['net_sse_mean']] = df3[['net_sse_mean']]

        subfiga.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
        subfiga.update_layout(width=1000,height=700)
        st.write(subfiga)
        st.write(df_sse.corr(method='pearson'))

        
        
        subfig = make_subplots(specs=[[{"secondary_y": True}]])
        fig = px.line(df_sthbd, x='date', y=df_sthbd.filter(items=['hsi']).columns)
        fig2 = px.line(df_mean, x='date', y=df_mean.filter(items=['net_szse_mean']).columns)
        
        fig2.update_traces(yaxis="y2")

        subfig.add_traces(fig.data + fig2.data)
        subfig.layout.xaxis.title="Date"
        subfig.update_yaxes(title_text="<b>Heng Seng Index</b>", secondary_y=False)
        subfig.update_yaxes(title_text="<b>Net Southbound Mean (SZ)</b><br>(HK$ million)", secondary_y=True)
        
        subfig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        
        df_szse=pd.DataFrame()
        df_szse[['hsi']] = df2[['hsi']]
        df_szse[['net_szse_mean']] = df3[['net_szse_mean']]

        subfig.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
        subfig.update_layout(width=1000,height=700)
        st.write(subfig)
        st.write(df_szse.corr(method='pearson')) 
        #st.dataframe(df_sthbd)


if __name__ == '__main__':
    main()
