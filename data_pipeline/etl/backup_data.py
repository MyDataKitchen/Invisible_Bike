from data_pipeline.models.s3 import get_s3_temp_filenames, get_data_from_s3, move_temp_file
from data_pipeline.models.mongo import insert_s3_temp_to_mongo
from datetime import datetime
import os
import threading

TAIPEI_TEMP = os.getenv('TAIPEI_YOUBIKE_TEMP_DIRECTORY_PATH')
TAICHUNG_TEMP = os.getenv('TAICHUNG_YOUBIKE_TEMP_DIRECTORY_PATH')
WEATHER_TEMP = os.getenv('WEATHER_TEMP_DIRECTORY_PATH')
PRECIPITATION_TEMP = os.getenv('PRECIPITATION_TEMP_DIRECTORY_PATH')

TAIPEI = "taipei"
TAICHUNG = "taichung"
WEATHER = "weather"
PRECIPITATION = "precipitation"


def move_youbike_temp(source, path):
    filenames = get_s3_temp_filenames(path)

    if filenames == False:
        print(f"{ source } - No temp data")
        return False

    for filename in filenames:
        data = get_data_from_s3(filename)
        if data != None:
            updated_time = {"taipei": data[0]['srcUpdateTime'], "taichung": data['updated_at']}
        data = {"created_at": updated_time[source], "item": data, "filename": filename}
        status = insert_s3_temp_to_mongo(source, data)
        if status == True:
            print(f"{ source } - Insert to MongDB successful")
            origin = filename
            destination = filename.split('/', 1)[1]
            move_temp_file(origin, destination)
        else:
            print(f"{ source } - Failed to write to MongDB")

    return True


def move_weather_temp(source, path):
    filenames = get_s3_temp_filenames(path)
    if filenames == False:
        print(f"{ source } - No temp data")
        return False

    for filename in filenames:
        data = get_data_from_s3(filename)
        if data != None:
            updated_time = {"weather": data['records']['location'][0]['time']['obsTime'],
                            "precipitation": datetime.strptime(data['cwbopendata']['location'][0]['time']['obsTime'], "%Y-%m-%dT%H:%M:%S+08:00").strftime('%Y-%m-%d %H:%M:%S')}
        data = {"created_at": updated_time[source], "item": data, "filename": filename}
        status = insert_s3_temp_to_mongo(source, data)
        if status == True:
            print(f"{ source } - Insert to MongDB successful")
            origin = filename
            destination = filename.split('/', 1)[1]
            move_temp_file(origin, destination)
        else:
            print(f"{ source } - Failed to write to MongDB")

    return True

if __name__ == '__main__':
    tasks = [(move_youbike_temp, TAIPEI, TAIPEI_TEMP), (move_youbike_temp, TAICHUNG, TAICHUNG_TEMP), (move_weather_temp, WEATHER, WEATHER_TEMP), (move_weather_temp, PRECIPITATION, PRECIPITATION_TEMP)]
    threads = []

    for task in tasks:
        threads.append(threading.Thread(target=task[0], args=(task[1], task[2],)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()



