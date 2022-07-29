import os
# os.environ["MODIN_ENGINE"] = "dask"
os.environ["MODIN_ENGINE"] = "ray"
from models.s3 import get_parquet, get_dates_s3
from models.mysql import get_all_stations, get_date
from datetime import datetime as dt
import streamlit as st
import plotly.express as px
import datetime
import modin.pandas as pd
import plotly.figure_factory as ff
import plotly.graph_objects as go
from PIL import Image
import sys
from streamlit import cli as stcli


@st.experimental_memo(show_spinner=False, ttl=60)
def get_date_mysql():
    return get_date()


def show_date(date):
    days = {"Sunday": "星期天", "Monday": "星期一", "Tuesday": "星期二", "Wednesday": "星期三", "Thursday": "星期四", "Friday": "星期五",
            "Saturday": "星期六"}
    day = datetime.datetime.strptime(str(date), '%Y-%m-%d').strftime('%A')
    result = f"### { date } &nbsp;: &nbsp;{ days[day] }"
    return result


def select_city(city):
    citys = {'台北': 'taipei', '台中': 'taichung'}
    return citys[city]


@st.experimental_memo(show_spinner=False, ttl=600)
def load_parquet(city, date):
    df = get_parquet(f"parquet/{ city }/{ date }_{ city }.parquet")
    df['日期時間'] = pd.to_datetime(df['日期時間'])
    return df


@st.experimental_memo(show_spinner=False, ttl=60)
def load_all_stations(city):
    df, stations_id = get_all_stations(city)
    return df, stations_id


@st.experimental_memo(show_spinner=False, ttl=60)
def get_districts(df):
    districts = df[['區域']].drop_duplicates()['區域'].tolist() #get unique districts from dataframe
    return districts


@st.experimental_memo(show_spinner=False, ttl=60)
def get_onservice_stations(df):
    stations = df['借用站編號'].drop_duplicates().tolist()
    return stations


@st.experimental_memo(show_spinner=False, ttl=60)
def get_times(df):
    times = pd.to_datetime(df["時間"]).drop_duplicates().dt.strftime('%H:%M').values.tolist()
    times.insert(0, times[0])
    return times


@st.cache(show_spinner=False, max_entries=3, ttl=60)
def districts_selected(df, districts):
    df = df[df['區域'].isin(districts)]
    return df


def time_selected(df, time):
    df = df[df['時間'] == dt.strptime(time, '%H:%M').time().strftime("%H:%M:%S")]
    return df


@st.cache(show_spinner=False, suppress_st_warning=True, ttl=60)
def daily_usage(df):
    df = df.groupby(pd.Grouper(key='日期時間', axis=0, freq='30Min')).agg(
        {'歸還數量': 'sum', '借出數量': 'sum'}).reset_index()
    return df


@st.cache(show_spinner=False, suppress_st_warning=True, ttl=60)
def district_usage(df):
    df = df.groupby("區域").agg({'借出數量': 'sum', '歸還數量': 'sum'}).reset_index()
    districts = df["區域"].tolist()
    outPerMinute = df["借出數量"].tolist()
    inPerMinute = df["歸還數量"].tolist()
    return districts, outPerMinute, inPerMinute


@st.cache(show_spinner=False, suppress_st_warning=True, ttl=60)
def daily_district_usage(df):
    df = df.groupby(['區域', pd.Grouper(key='日期時間', freq='30Min')]).agg(
        {'借出數量': 'sum', '歸還數量': 'sum'}).reset_index()
    return df


@st.cache(show_spinner=False, suppress_st_warning=True, ttl=60)
def station_number(df):
    df = df.groupby(['區域']).agg({'借用站編號': 'count'}).reset_index()
    districts = df['區域'].tolist()
    count = df['借用站編號'].tolist()
    return districts, count


def main():
    favicon = Image.open("static/favicon.ico")
    st.set_page_config(
         page_title="Invisible Bike",
         page_icon=favicon,
         layout="wide",
         initial_sidebar_state="expanded"
     )

    st.sidebar.title("INVISIBLE BIKES EXPLORER")
    st.sidebar.markdown('---')

    city = st.sidebar.selectbox(
      '選取縣市',
      ['台北', '台中'])

    dates = get_dates_s3(f"parquet/{ select_city(city) }/")

    date = st.sidebar.date_input(
        "選取日期",
        get_date_mysql(), min_value=dt.strptime(dates[0], '%Y-%m-%d'), max_value=get_date_mysql())


    if str(date) in dates:

        df = load_parquet(select_city(city), date)

        station_df, stations_id = load_all_stations(city)

        onservice = get_onservice_stations(df)

        times = get_times(df)

        select_district = st.sidebar.multiselect(
          '選取區域',
          get_districts(df), get_districts(df)[0:10])

        col1, col2, col3 = st.columns([5,6,1])
        col1.write(show_date(date))

        col3.caption(f"更新時間:", unsafe_allow_html=False)
        col3.caption(f"{ get_date_mysql().strftime('%Y-%m-%d %H:%M') }", unsafe_allow_html=False)
        st.markdown("---")

        def show_data(onservice, stations_id):
            col2.metric(label="服務中的數量 / 借用站總量", value = f"{ len(onservice) } / { len(stations_id) }",
                        delta_color="off")


        def show_map(df, shortage_duration):
            temp_df = df[(df['缺車的時間長度'] >= shortage_duration[0]) & (df['缺車的時間長度'] <= shortage_duration[1])]
            fig = px.scatter_mapbox(temp_df, lat="緯度", lon="經度", hover_name="借用站名稱",
                                    hover_data=["可借用腳踏車數量", "空位數量", "區域"],
                                    color="腳踏車供應狀況",size="停車位數量", size_max=11,
                                    zoom=11, height=600, opacity=.8,color_discrete_map={'red': '#EF553E',
                                         'yellow':'#FECB52', 'green':'#2CA02C'})
            fig.update_layout(mapbox_style="carto-positron", autosize=True, margin={"r": 0, "t": 0, "l": 0, "b": 0},hovermode='closest')
            col5.plotly_chart(fig, use_container_width=True)
            col5.caption("* 地圖中的圖示大小，代表的是該借用站的車位數量", unsafe_allow_html=False)
            col5.text(" \n")


        def show_temporarily_closed(df, station_df, stations_id):
            temporarily_closed_stations = station_df[station_df['stationId'].isin(list(set(stations_id) - set(get_onservice_stations(df))))].values.tolist()
            st.markdown("---")
            st.markdown(f"##### { city }市目前暫停服務的借用站")
            st.text(" \n")
            if temporarily_closed_stations == []:
                st.info('目前全部借用站已投入服務')
            else:
                temporarily_closed_stations.insert(0, ['借用站編號', '借用站名稱', '地址', '區域'])
                fig = ff.create_table(temporarily_closed_stations)
                st.plotly_chart(fig, use_container_width=True)


        def chart_1(df):
            st.markdown(f'##### {city}市每小時的使用狀況')

            fig = px.line(df, x='日期時間', y=['歸還數量', '借出數量'], height=500)
            fig.update_layout(
                xaxis_title="時間(小時)",
                yaxis_title="使用量",
                legend_title=""
            )
            fig.update_xaxes(title_text='時間(小時)')
            fig.update_yaxes(title_text='使用量')
            newnames = {'歸還數量': "歸還數量", '借出數量': "借出數量"}
            fig.for_each_trace(lambda t: t.update(name=newnames[t.name],
                                                  legendgroup=newnames[t.name],
                                                  hovertemplate=t.hovertemplate.replace(t.name, newnames[t.name])
                                                  )
                               )
            st.plotly_chart(fig, use_container_width=True)


        def chart_2(df):
            st.markdown(f'##### {city}市各區每小時的使用狀況')
            fig = px.line(df, x='日期時間', y=['借出數量'], color="區域", height=500)
            fig.update_layout(
                xaxis_title="時間(小時)",
                yaxis_title="各區域使用量",
                legend_title=""
            )
            st.plotly_chart(fig, use_container_width=True)


        def chart_3(districts, outPerMinute, inPerMinute):
            st.markdown(f'##### {city}市各區的使用狀況')
            fig = go.Figure(data=[
                go.Bar(name='歸還數量', x=districts, y=outPerMinute, text=outPerMinute),
                go.Bar(name='借出數量', x=districts, y=inPerMinute, text=inPerMinute)
            ])
            fig.update_layout(barmode='group',
                              height=500,
                              xaxis={'categoryorder': 'total ascending'},
                              xaxis_title="",
                              yaxis_title="使用量",
                              legend_title=""
                              )
            st.plotly_chart(fig, use_container_width=True)
            st.write(str(date))


        if select_district != []:
            col5, col6 = st.columns([100, 1])
            time = st.select_slider(
                 '選取時間',
                 options=times)

            filter_by_time_df = time_selected(df, time)
            filter_by_district_df = districts_selected(filter_by_time_df, select_district)

            shortage_duration = st.slider(
                '缺車時間長度',
                int(filter_by_district_df["缺車的時間長度"].min()), int(filter_by_district_df["缺車的時間長度"].max()),
                (int(filter_by_district_df["缺車的時間長度"].min()), int(filter_by_district_df["缺車的時間長度"].max())), step=10, key=3)

            show_data(onservice, stations_id)
            show_map(filter_by_district_df, shortage_duration)
            show_temporarily_closed(df, station_df, stations_id)

            st.markdown("---")
            chart_1(daily_usage(df))
            chart_2(daily_district_usage(df))
            chart_3(district_usage(df)[0], district_usage(df)[1], district_usage(df)[2])

        else:
            st.info('未找到相關資訊。')

    else:
        st.info('未找到相關資訊。')


if __name__ == '__main__':
    if st._is_running_with_streamlit:
        main()
    else:
        sys.argv = ["streamlit", "run", sys.argv[0]]
        sys.exit(stcli.main())























