import os
os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray
import ray
ray.init(ignore_reinit_error=True,object_store_memory=2000 * 1024 * 1024)
from models.s3 import get_parquet_from_s3
from models.mysql import get_all_stations
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime as dt
import streamlit as st
import plotly.express as px
import datetime
import modin.pandas as pd
import plotly.figure_factory as ff
import plotly.graph_objects as go


load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')

engine = create_engine(f"mysql+pymysql://{ SQL_USER }:{ SQL_PASSWORD }@{ SQL_HOST }/{ SQL_DATABASE }",
                       pool_size=10, max_overflow=5, pool_pre_ping=True)

#### Title ####

st.set_page_config(layout="wide")
st.sidebar.title("INVISIBLE BIKE EXPLORER")
st.sidebar.markdown('---')


#### Sidebar Select Date And City ####

city = st.sidebar.selectbox(
  '選取縣市',
  ['台北', '台中'])

date = st.sidebar.date_input(
          "選取日期",
          datetime.date(2022, 6, 28), min_value=datetime.date(2022, 6, 26), max_value=datetime.date(2022, 6, 28))


def show_date(date):
    days = {"Sunday": "星期天", "Monday": "星期一", "Tuesday": "星期二", "Wednesday": "星期三", "Thursday": "星期四", "Friday": "星期五",
            "Saturday": "星期六"}
    day = datetime.datetime.strptime(str(date), '%Y-%m-%d').strftime('%A')
    result = f"### { date } &nbsp;: &nbsp;{ days[day] }"
    return result


def select_city(city):
    citys = {'台北': 'taipei', '台中': 'taichung'}
    return citys[city]

#### Get Parquet From S3 ####

# @st.cache(show_spinner=False, max_entries=5, ttl=60)
@st.experimental_memo
def load_parquet(city, date):
    df = get_parquet_from_s3("invisible-bike", f"parquet/{ city }/{ date }_{ city }.parquet")
    df['datetime'] = pd.to_datetime(df['datetime'])
    return df

df = load_parquet(select_city(city), date)

# @st.cache(show_spinner=False, max_entries=3, ttl=60)
@st.experimental_memo
def get_districts(df):
    districts = df[['district']].drop_duplicates()['district'].tolist() #get unique districts from dataframe
    return districts

# @st.cache(show_spinner=False, max_entries=3, ttl=60)
@st.experimental_memo
def get_onservice_stations(df):
    stations = df['stationId'].drop_duplicates().tolist()
    return stations

onservice = get_onservice_stations(df)

# @st.cache(show_spinner=False, max_entries=3, ttl=60)
@st.experimental_memo
def times(df):
    times = pd.to_datetime(df["time"]).drop_duplicates().dt.strftime('%H:%M').values.tolist()
    times.insert(0, times[0])
    return times

times = times(df)

@st.cache(show_spinner=False, max_entries=3, ttl=60)
def districts_selected(df, districts):
    df = df[df['district'].isin(districts)]
    return df


def time_selected(df, time):
    df = df[df['time'] == dt.strptime(time, '%H:%M').time().strftime("%H:%M:%S")]
    return df


@st.cache(show_spinner=False, suppress_st_warning=True)
def daily_usage(df):
    df = df.groupby(pd.Grouper(key='datetime', axis=0, freq='1h')).agg(
        {'inPerMinute': 'sum', 'outPerMinute': 'sum'}).reset_index()
    return df


@st.cache(show_spinner=False, suppress_st_warning=True)
def district_usage(df):
    df = df.groupby("district").agg({'outPerMinute': 'sum', 'inPerMinute': 'sum'}).reset_index()
    districts = df["district"].tolist()
    outPerMinute = df["outPerMinute"].tolist()
    inPerMinute = df["inPerMinute"].tolist()
    return districts, outPerMinute, inPerMinute


@st.cache(show_spinner=False, suppress_st_warning=True)
def daily_district_usage(df):
    df = df.groupby(['district', pd.Grouper(key='datetime', freq='1h')]).agg(
        {'outPerMinute': 'sum', 'inPerMinute': 'sum'}).reset_index()
    return df

@st.cache(show_spinner=False, suppress_st_warning=True)
def station_number(df):
    df = df.groupby(['district']).agg({'stationId': 'count'}).reset_index()
    districts = df['district'].tolist()
    count = df['stationId'].tolist()
    return districts, count



select_district = st.sidebar.multiselect(
  '選取區域',
  get_districts(df), get_districts(df)[0:10])


### Show Date ####
st.write(show_date(date))
st.markdown("---")


### Show Data ####


def show_data(df, current_df, times, time):
    index = times.index(time, 1)
    previous_time = times[index - 1]

    current_return = current_df['inPerMinute'].sum()
    current_lend = current_df['outPerMinute'].sum()

    pre_df = df[df['time'] == dt.strptime(previous_time, '%H:%M').time().strftime("%H:%M:%S")]
    pre_return = pre_df['inPerMinute'].sum()
    pre_lend = pre_df['outPerMinute'].sum()

    temp, stations_id = get_all_stations(city) #all stations ID
    del temp

    col1.metric(label="借用站數量", value = len(stations_id),
                delta_color="inverse")


    col2.metric(label="服務中的借用站數量", value = len(onservice),
                delta_color="off")

    if round(float((current_return - pre_return) / current_return), 2) >= 0 and round(float((current_return - pre_return) / current_return), 2) <= 100:
        return_result = round(float((current_return - pre_return) / current_return), 2)
    else:
        return_result = 0
        pass

    col3.metric(label="歸還數量", value = current_df['inPerMinute'].sum(),
                delta=f"{ return_result } %",
                delta_color="normal")

    if round(float((current_lend - pre_lend) /  current_lend), 2) >= 0 and round(float((current_lend - pre_lend) /  current_lend), 2) <= 100:
        lend_result = round(float((current_lend - pre_lend) /  current_lend), 2)
    else:
        lend_result = 0
        pass

    col4.metric(label="借出數量", value = current_df['outPerMinute'].sum(),
                delta=f"{ lend_result } %",
                delta_color="normal")


def show_map(df, select_info, available_lots, shortage_lots, shortage_duration):
    data_layout = {"可停數量":"total", "可借車輛":"availableSpace", "可停空位":"emptySpace"}

    temp_df = df[((df['proportion'] >= available_lots[0]) & (df['proportion'] <= available_lots[1]))]
    temp_df = temp_df[(temp_df['shortageDuration'] >= shortage_duration[0]) & (temp_df['shortageDuration'] <= shortage_duration[1])]

    fig = px.scatter_mapbox(temp_df, lat="lat", lon="lon", hover_name="name",
                            hover_data=["total", "emptySpace", "district", "proportion", "availableSpace"],
                            color="proportion", size = data_layout[select_info],
                            zoom=11, height=680, opacity=.8, color_continuous_scale="plotly3")
    fig.update_layout(mapbox_style="carto-positron", autosize=True, margin={"r": 0, "t": 0, "l": 0, "b": 0},hovermode='closest')

    col5.plotly_chart(fig, use_container_width=True)


def show_temporarily_closed(df):
    df_all, stations_id = get_all_stations(city)
    temporarily_closed_stations = df_all[df_all['stationId'].isin(list(set(stations_id) - set(get_onservice_stations(df))))].values.tolist()
    st.markdown("---")
    st.markdown("##### 目前暫停服務的借用站")
    st.text(" \n")
    if temporarily_closed_stations == []:
        st.info('目前全部借用站已投入服務')
    else:
        temporarily_closed_stations.insert(0, ['借用站編號', '借用站名稱', '地址', '區域'])
        fig = ff.create_table(temporarily_closed_stations)
        st.plotly_chart(fig, use_container_width=True)


def chart_1(df):
    col7.markdown('##### 整體縣市每小時的使用狀況')

    fig = px.line(df, x='datetime', y=['inPerMinute', 'outPerMinute'], height=500)
    newnames = {'inPerMinute': "腳踏車歸還 / 小時", 'outPerMinute': "腳踏車借出 / 小時"}
    fig.for_each_trace(lambda t: t.update(name=newnames[t.name],
                                          legendgroup=newnames[t.name],
                                          hovertemplate=t.hovertemplate.replace(t.name, newnames[t.name])
                                          )
                       )
    col7.plotly_chart(fig, use_container_width=True)


def chart_2(districts, outPerMinute, inPerMinute):
    col8.markdown('##### 各市區的使用狀況')
    fig = go.Figure(data=[
        go.Bar(name='腳踏車歸還 / 區域', x=districts, y=outPerMinute, text=outPerMinute),
        go.Bar(name='腳踏車借出 / 區域', x=districts, y=inPerMinute, text=inPerMinute)
    ])
    fig.update_layout(barmode='group', height=500, xaxis={'categoryorder': 'total ascending'})
    col8.plotly_chart(fig, use_container_width=True)


def chart_3(df):
    col9.markdown('##### 各市區每小時的使用狀況')
    fig = px.line(df, x='datetime', y=['outPerMinute'], color="district", height=500)
    col9.plotly_chart(fig, use_container_width=True)


def chart_4(districts, count):
    col10.markdown('##### 各市區的借用站數量')
    fig = go.Figure(data=[
        go.Bar(name='腳踏車歸還 / 區域', x=districts, y=count, text=count)
    ])
    fig.update_layout(barmode='group', height=500, xaxis={'categoryorder': 'total ascending'})
    col10.plotly_chart(fig, use_container_width=True)

def chart_5():
    pass


if select_district != []:
    col1, col2, col3, col4 = st.columns(4) #show show_data function

    st.text(" \n")
    st.text(" \n")

    col5, col6 = st.columns([4, 1])

    #### Map Container #####

    select_info = col6.selectbox(
      '選取資料圖層',
        ("可停數量", "可借車輛", "可停空位"))


    time = col6.select_slider(
         '選取時間',
         options=times)


    available_lots = col6.slider(
        '可用停車位與停車位數量的比例',
        0, 100, (0, 100), step=5, key=1)


    shortage_lots = col6.slider(
        '停車位空缺與停車位數量的比例',
        0, 100, (0, 100), step=5, key=2)


    filter_by_time_df = time_selected(df, time)
    filter_by_district_df = districts_selected(filter_by_time_df, select_district)


    shortage_duration = col6.slider(
        '缺車時間長度',
        int(filter_by_district_df["shortageDuration"].min()), int(filter_by_district_df["shortageDuration"].max()),
        (int(filter_by_district_df["shortageDuration"].min()), int(filter_by_district_df["shortageDuration"].max())), step=10, key=3)


    show_data(df, filter_by_time_df, times, time)
    show_map(filter_by_district_df,select_info, available_lots, shortage_lots, shortage_duration)
    show_temporarily_closed(df)

    st.markdown("---")
    col7, col8 = st.columns(2)
    chart_1(daily_usage(df))
    chart_2(district_usage(df)[0], district_usage(df)[1], district_usage(df)[2])

    col9, col10 = st.columns(2)
    chart_3(daily_district_usage(df))
    chart_4(station_number(filter_by_time_df)[0], station_number(filter_by_time_df)[1])

else:
    st.info('未找到相關資訊。')























