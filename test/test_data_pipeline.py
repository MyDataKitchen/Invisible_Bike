from unittest.mock import patch
from data_pipeline.etl.transform_data import shortage_status, station_event
from data_pipeline.etl.load_data import json_converter



def test_shortage_status():
    proportion = 29
    assert shortage_status(proportion) == "red"

def test_station_event():
    datetime = '2022-07-22 10:31:11'

    temp = {'datetime': '2022-07-22 10:24:12',
            'stationId': 500101001,
            'total': 28,
            'availableSpace': 2,
            'emptySpace': 26,
            'status': 1,
            'outPerMinute': 0,
            'inPerMinute': 0,
            'color': 'red',
            'shortageDuration': 190}

    data = {'act': '1',
            'ar': '復興南路二段235號前',
            'aren': 'No.235， Sec. 2， Fuxing S. Rd.',
            'bemp': 27,
            'infoDate': '2022-07-22',
            'infoTime': '2022-07-22 10:30:30',
            'lat': 25.02605,
            'lng': 121.5436,
            'mday': '2022-07-22 10:30:30',
            'sarea': '大安區',
            'sareaen': 'Daan Dist.',
            'sbi': 1,
            'sna': 'YouBike2.0_捷運科技大樓站',
            'snaen': 'YouBike2.0_MRT Technology Bldg. Sta.',
            'sno': '500101001',
            'srcUpdateTime': '2022-07-22 10:31:11',
            'tot': 28,
            'updateTime': '2022-07-22 10:31:24'}

    assert station_event(datetime, temp, data) == {'datetime': '2022-07-22 10:31:11',
                                                      'stationId': 500101001,
                                                      'total': 28,
                                                      'availableSpace': 1,
                                                      'emptySpace': 27,
                                                      'status': 1,
                                                      'outPerMinute': 1,
                                                      'inPerMinute': 0,
                                                      'color': 'red',
                                                      'shortageDuration': 191}

def test_json_converter():
    data = [('2022-07-10', '12:17:00', '2022-07-10 12:17:00', 500101001, '捷運科技大樓站', 29, 2, 27, 0, 0, 'red', 1, 25.0261, 121.544, '大安區')]
    assert json_converter(data) == [{'日期': '2022-07-10',
                                    '時間': '12:17:00',
                                    '日期時間': '2022-07-10 12:17:00',
                                    '借用站編號': 500101001,
                                    '借用站名稱': '捷運科技大樓站',
                                    '停車位數量': 29,
                                    '可借用腳踏車數量': 2,
                                    '空位數量': 27,
                                    '借出數量': 0,
                                    '歸還數量': 0,
                                    '腳踏車供應狀況': 'red',
                                    '缺車的時間長度': 1,
                                    '緯度': 25.0261,
                                    '經度': 121.544,
                                    '區域': '大安區'}]






