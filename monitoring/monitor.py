import streamlit as st
from models.mysql import crawler_logs, mysql_logs, get_date
import plotly.express as px
import pandas as pd
import datetime
from PIL import Image
import sys
from streamlit import cli as stcli

def main():

    favicon = Image.open("static/favicon.ico")
    st.set_page_config(
         page_title="Invisible Bikes",
         page_icon=favicon,
         layout="wide",
         initial_sidebar_state="expanded"
     )

    st.sidebar.title("INVISIBLE BIKES EXPLORER")
    st.sidebar.markdown('---')

    date = st.sidebar.date_input(
              "選取日期",
              get_date(), min_value=datetime.date(2022, 7, 15), max_value=get_date())

    @st.experimental_memo(show_spinner=False, ttl=60)
    def taipei_crawler_logs(date, city):
        df = crawler_logs(date.strftime("%Y-%m-%d"), city)
        df = df.groupby(pd.Grouper(key='Datetime', axis=0, freq='3Min')).agg(
            {'Data Length': 'max', 'Data Size': 'max', 'Respone Time (s)': 'max', 'Insert Time (s)': 'max'}).reset_index()
        return df

    @st.experimental_memo(show_spinner=False, ttl=60)
    def taichung_crawler_logs(date, city):
        df = crawler_logs(date.strftime("%Y-%m-%d"), city)
        df = df.groupby(pd.Grouper(key='Datetime', axis=0, freq='3Min')).agg(
            {'Data Length': 'max', 'Data Size': 'max', 'Respone Time (s)': 'max', 'Insert Time (s)': 'max'}).reset_index()
        return df

    @st.experimental_memo(show_spinner=False, ttl=60)
    def taipei_mysql_logs(date, city):
        df = mysql_logs(date.strftime("%Y-%m-%d"), city)
        df = df.groupby(pd.Grouper(key='Datetime', axis=0)).agg(
            {'Querying Time (s)': 'max', 'Transforming Time (s)': 'max', 'Insert Time (s)': 'max'}).reset_index()
        return df

    @st.experimental_memo(show_spinner=False, ttl=60)
    def taichung_mysql_logs(date, city):
        df = mysql_logs(date.strftime("%Y-%m-%d"), city)
        df = df.groupby(pd.Grouper(key='Datetime', axis=0)).agg(
            {'Querying Time (s)': 'max', 'Transforming Time (s)': 'max', 'Insert Time (s)': 'max'}).reset_index()
        return df


    @st.experimental_memo(show_spinner=False, ttl=60)
    def check_duplicate_data(date, city):
        df = crawler_logs(date.strftime("%Y-%m-%d"), city)
        df = df[['Insert Time (s)','Status']].groupby(['Status']).count().rename(columns={'Insert Time (s)': 'Status'}, index={0: 'Duplicate', 1: 'Available Data'})
        return df

    @st.experimental_memo(show_spinner=False, ttl=60)
    def get_date_mysql():
        return get_date()


    df_tp_crawler = taipei_crawler_logs(date, "taipei")
    df_tc_crawler = taichung_crawler_logs(date, "taichung")
    df_tp_mysql = taipei_mysql_logs(date, "taipei")
    df_tc_mysql = taichung_mysql_logs(date, "taichung")
    df_tp = check_duplicate_data(date, "taipei")
    df_tc = check_duplicate_data(date, "taichung")

    col1, col2 = st.columns([10, 1])

    col1.title("資料流處理 - 監控系統")

    col2.caption(f"更新時間:", unsafe_allow_html=False)
    col2.caption(f"{ get_date_mysql().strftime('%Y-%m-%d %H:%M') }", unsafe_allow_html=False)

    st.markdown("---")

    st.subheader('台北 YOUBIKE 資料請求系統')

    def show_respone_time(df):
        fig = px.line(df, x='Datetime', y='Respone Time (s)', title='資料請求時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#636EFA')
        st.plotly_chart(fig, use_container_width=True)

    show_respone_time(df_tp_crawler)

    def show_respone_time(df):
        fig = px.line(df, x='Datetime', y='Insert Time (s)', title='資料寫入時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#636EFA')
        st.plotly_chart(fig, use_container_width=True)

    show_respone_time(df_tp_crawler)

    def show_respone_time(df):
        fig = px.line(df, x='Datetime', y='Data Length', title='每筆資料的數量 (個)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#636EFA')
        st.plotly_chart(fig, use_container_width=True)

    show_respone_time(df_tp_crawler)

    st.subheader('台中 YOUBIKE 資料請求系統')

    def show_respone_time(df):
        fig = px.line(df, x='Datetime', y='Respone Time (s)', title='資料請求時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#FF7F0E')
        st.plotly_chart(fig, use_container_width=True)

    show_respone_time(df_tc_crawler)

    def show_respone_time(df):
        fig = px.line(df, x='Datetime', y='Insert Time (s)', title='資料寫入時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#FF7F0E')
        st.plotly_chart(fig, use_container_width=True)

    show_respone_time(df_tc_crawler)

    def show_respone_time(df):
        fig = px.line(df, x='Datetime', y='Data Length', title='每筆資料的數量 (個)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#FF7F0E')
        st.plotly_chart(fig, use_container_width=True)

    show_respone_time(df_tc_crawler)

    st.markdown("---")
    st.subheader('台北 YOUBIKE 資料處理系統')

    def show_processing_time(df):
        fig = px.line(df, x='Datetime', y='Querying Time (s)', title='請求 MYSQL 的時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#636EFA')
        st.plotly_chart(fig, use_container_width=True)

    show_processing_time(df_tp_mysql)

    def show_processing_time(df):
        fig = px.line(df, x='Datetime', y='Transforming Time (s)', title='資料運算及轉換的時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#636EFA')
        st.plotly_chart(fig, use_container_width=True)

    show_processing_time(df_tp_mysql)

    def show_processing_time(df):
        fig = px.line(df, x='Datetime', y='Insert Time (s)', title='資料存入至 S3 的時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#636EFA')
        st.plotly_chart(fig, use_container_width=True)

    show_processing_time(df_tp_mysql)

    st.subheader('台中 YOUBIKE 資料處理系統')

    def show_processing_time(df):
        fig = px.line(df, x='Datetime', y='Querying Time (s)', title='請求 MYSQL 的時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#FF7F0E')
        st.plotly_chart(fig, use_container_width=True)

    show_processing_time(df_tc_mysql)

    def show_processing_time(df):
        fig = px.line(df, x='Datetime', y='Transforming Time (s)', title='資料運算及轉換的時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#FF7F0E')
        st.plotly_chart(fig, use_container_width=True)

    show_processing_time(df_tc_mysql)

    def show_processing_time(df):
        fig = px.line(df, x='Datetime', y='Insert Time (s)', title='資料存入至 S3 的時間 (秒)')
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_traces(line_color='#FF7F0E')
        st.plotly_chart(fig, use_container_width=True)

    show_processing_time(df_tc_mysql)

    st.markdown("---")
    st.subheader('YOUBIKE 資料重複的比例')

    col1, col2 = st.columns(2)

    def show_duplicate(df):
        fig = px.pie(df, values='Status', names=df.index,
                     title='台北 YOUBIKE 重複資料的比例', color=df.index, color_discrete_map={'Available Data': '#636EFA',
                                     'Duplicate':'#EF553B'})

        fig.update_traces(textposition='inside', textinfo='percent+label')
        col1.plotly_chart(fig, use_container_width=True)

    show_duplicate(df_tp)

    def show_duplicate(df):
        fig = px.pie(df, values='Status', names=df.index,
                     title='台中 YOUBIKE 重複資料的比例', color=df.index, color_discrete_map={'Available Data': '#FF7F0E',
                                     'Duplicate':'#2CA02C'})

        fig.update_traces(textposition='inside', textinfo='percent+label')
        col2.plotly_chart(fig, use_container_width=True)

    show_duplicate(df_tc)

if __name__ == '__main__':
    if st._is_running_with_streamlit:
        main()
    else:
        sys.argv = ["streamlit", "run", sys.argv[0]]
        sys.exit(stcli.main())


