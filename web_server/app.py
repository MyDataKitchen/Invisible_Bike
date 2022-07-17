import os
# os.environ["MODIN_ENGINE"] = "dask"
os.environ["MODIN_ENGINE"] = "ray"

from models.s3 import get_parquet_from_s3
from models.mysql import get_all_stations, get_date
from dotenv import load_dotenv
from sqlalchemy import create_engine
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


load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')

engine = create_engine(f"mysql+pymysql://{ SQL_USER }:{ SQL_PASSWORD }@{ SQL_HOST }/{ SQL_DATABASE }",
                       pool_size=20, max_overflow=5, pool_pre_ping=True)

#### Title ####
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


    #### Sidebar Select Date And City ####

    city = st.sidebar.selectbox(
      '選取縣市',
      ['台北', '台中'])

    date = st.sidebar.date_input(
              "選取日期",
              datetime.date(2022, 7, 20), min_value=datetime.date(2022, 7, 11), max_value=datetime.date(2022, 7, 20))


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
    @st.experimental_memo(show_spinner=False, ttl=600) #add cache without showing loading
    def load_parquet(city, date):
        df = get_parquet_from_s3("invisible-bikes", f"parquet/{ city }/{ date }_{ city }.parquet")

        df['日期時間'] = pd.to_datetime(df['日期時間'])

        return df


    df = load_parquet(select_city(city), date)

    # @st.cache(show_spinner=False, max_entries=3, ttl=60)
    @st.experimental_memo(show_spinner=False, ttl=600)
    def get_districts(df):
        districts = df[['區域']].drop_duplicates()['區域'].tolist() #get unique districts from dataframe
        return districts

    # @st.cache(show_spinner=False, max_entries=3, ttl=60)
    @st.experimental_memo(show_spinner=False, ttl=600)
    def get_onservice_stations(df):
        stations = df['借用站編號'].drop_duplicates().tolist()
        return stations

    onservice = get_onservice_stations(df)

    # @st.cache(show_spinner=False, max_entries=3, ttl=60)
    @st.experimental_memo(show_spinner=False, ttl=600)
    def times(df):
        times = pd.to_datetime(df["時間"]).drop_duplicates().dt.strftime('%H:%M').values.tolist()
        times.insert(0, times[0])
        return times

    times = times(df)

    @st.cache(show_spinner=False, max_entries=3, ttl=600)
    def districts_selected(df, districts):
        df = df[df['區域'].isin(districts)]
        return df


    def time_selected(df, time):
        df = df[df['時間'] == dt.strptime(time, '%H:%M').time().strftime("%H:%M:%S")]
        return df


    @st.cache(show_spinner=False, suppress_st_warning=True, ttl=600)
    def daily_usage(df):
        df = df.groupby(pd.Grouper(key='日期時間', axis=0, freq='30Min')).agg(
            {'歸還數量': 'sum', '借出數量': 'sum'}).reset_index()
        return df


    @st.cache(show_spinner=False, suppress_st_warning=True, ttl=600)
    def district_usage(df):
        df = df.groupby("區域").agg({'借出數量': 'sum', '歸還數量': 'sum'}).reset_index()
        districts = df["區域"].tolist()
        outPerMinute = df["借出數量"].tolist()
        inPerMinute = df["歸還數量"].tolist()
        return districts, outPerMinute, inPerMinute


    @st.cache(show_spinner=False, suppress_st_warning=True, ttl=600)
    def daily_district_usage(df):
        df = df.groupby(['區域', pd.Grouper(key='日期時間', freq='30Min')]).agg(
            {'借出數量': 'sum', '歸還數量': 'sum'}).reset_index()
        return df

    @st.cache(show_spinner=False, suppress_st_warning=True, ttl=600)
    def station_number(df):
        df = df.groupby(['區域']).agg({'借用站編號': 'count'}).reset_index()
        districts = df['區域'].tolist()
        count = df['借用站編號'].tolist()
        return districts, count



    select_district = st.sidebar.multiselect(
      '選取區域',
      get_districts(df), get_districts(df)[0:10])


    ### Show Date ####
    co1, co2, col3 = st.columns([4,6,1])

    co1.write(show_date(date))

    col3.caption(f"更新時間:", unsafe_allow_html=False)
    col3.caption(f"{get_date()}", unsafe_allow_html=False)
    st.markdown("---")


    ### Show Data ####



    def show_data(df, current_df, times, time):
        index = times.index(time, 1)
        previous_time = times[index - 1]

        current_return = current_df['歸還數量'].sum()
        current_lend = current_df['借出數量'].sum()

        pre_df = df[df['時間'] == dt.strptime(previous_time, '%H:%M').time().strftime("%H:%M:%S")]
        pre_return = pre_df['歸還數量'].sum()
        pre_lend = pre_df['借出數量'].sum()

        temp, stations_id = get_all_stations(city) #all stations ID
        del temp

        # col1.metric(label="借用站數量", value = len(stations_id),
        #             delta_color="inverse")


        co2.metric(label="服務中的數量 / 借用站總量", value = f"{ len(onservice) } / { len(stations_id) }",
                    delta_color="off")

        if round(float((current_return - pre_return) / current_return), 2) >= 0 and round(float((current_return - pre_return) / current_return), 2) <= 100:
            return_result = round(float((current_return - pre_return) / current_return), 2)
        else:
            return_result = 0
            pass

        # col2.metric(label="歸還數量", value = current_df['歸還數量'].sum())

        if round(float((current_lend - pre_lend) /  current_lend), 2) >= 0 and round(float((current_lend - pre_lend) /  current_lend), 2) <= 100:
            lend_result = round(float((current_lend - pre_lend) /  current_lend), 2)
        else:
            lend_result = 0
            pass

        # col3.metric(label="借出數量", value = current_df['借出數量'].sum())


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


    def show_temporarily_closed(df):
        df_all, stations_id = get_all_stations(city)
        temporarily_closed_stations = df_all[df_all['stationId'].isin(list(set(stations_id) - set(get_onservice_stations(df))))].values.tolist()
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

        fig = px.line(df, x='日期時間', y=['歸還數量', '借出數量'], height=450)
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
        fig = px.line(df, x='日期時間', y=['借出數量'], color="區域", height=450)
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


    if select_district != []:
        col1, col2, col3 = st.columns(3) #show show_data function

        st.text(" \n")
        st.text(" \n")

        col5, col6 = st.columns([100, 1])

        #### Map Container #####


        time = st.select_slider(
             '選取時間',
             options=times)


        filter_by_time_df = time_selected(df, time)
        filter_by_district_df = districts_selected(filter_by_time_df, select_district)




        shortage_duration = st.slider(
            '缺車時間長度',
            int(filter_by_district_df["缺車的時間長度"].min()), int(filter_by_district_df["缺車的時間長度"].max()),
            (int(filter_by_district_df["缺車的時間長度"].min()), int(filter_by_district_df["缺車的時間長度"].max())), step=10, key=3)

        show_data(df, filter_by_time_df, times, time)
        show_map(filter_by_district_df, shortage_duration)
        show_temporarily_closed(df)

        st.markdown("---")
        chart_1(daily_usage(df))
        chart_2(daily_district_usage(df))
        chart_3(district_usage(df)[0], district_usage(df)[1], district_usage(df)[2])


    else:
        st.info('未找到相關資訊。')


if __name__ == '__main__':
    if st._is_running_with_streamlit:
        main()
    else:
        sys.argv = ["streamlit", "run", sys.argv[0]]
        sys.exit(stcli.main())























