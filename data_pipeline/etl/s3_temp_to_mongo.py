from data_pipeline.models.s3 import get_s3_temp_filenames, get_data_from_s3, move_temp_file
from data_pipeline.models.mongodb import insert_s3_temp_to_mongo
import os
import threading

TAIPEI_TEMP = os.getenv('TAIPEI_YOUBIKE_TEMP_DIRECTORY_PATH')
TAICHUNG_TEMP = os.getenv('TAICHUNG_YOUBIKE_TEMP_DIRECTORY_PATH')
WEATHER_TEMP = os.getenv('WEATHER_TEMP_DIRECTORY_PATH')
PRECIPITATION_TEMP = os.getenv('PRECIPITATION_TEMP_DIRECTORY_PATH')


def taipei_extract_and_load(path):
    filenames = get_s3_temp_filenames(path)


    if filenames == False:
        print("Taipei - No temp data")
    else:
        for filename in filenames:
            data = get_data_from_s3(filename)
            if data == None:
                pass
            else:
                updated_time = data[0]['srcUpdateTime']
                print(updated_time)
            data = {"created_at": updated_time, "item": data, "filename": filename}
            source = "taipei"
            status = insert_s3_temp_to_mongo(source, data)
            if status == True:
                print("Taipei - Insert to MongDB successful")
                origin = filename
                destination = filename.split('/', 1)[1]
                move_temp_file(origin, destination)
            else:
                print("Taipei - Failed to write to MongDB")




def taichung_extract_and_load(path):
    filenames = get_s3_temp_filenames(path)

    if filenames == False:
        print("Taichung - No temp data")
    else:
        for filename in filenames:
            data = get_data_from_s3(filename)
            if data == None:
                pass
            else:
                updated_time = data['updated_at']
            data = {"created_at": updated_time, "item": data, "filename": filename}
            source = "taichung"
            status = insert_s3_temp_to_mongo(source, data)
            if status == True:
                print("Taichung - Insert to MongDB successful")
                origin = filename
                destination = filename.split('/', 1)[1]
                move_temp_file(origin, destination)
            else:
                print("Taichung - Failed to write to MongDB")



def weather_extract_and_load(path):
    filenames = get_s3_temp_filenames(path)
    if filenames == False:
        print("Weather - No temp data")
    else:
        for filename in filenames:
            data = get_data_from_s3(filename)
            if data == None:
                pass
            else:
                updated_time = data['records']['location'][0]['time']['obsTime']
            data = {"created_at": updated_time, "item": data, "filename": filename}
            source = "weather"
            status = insert_s3_temp_to_mongo(source, data)
            if status == True:
                print("Weather - Insert to MongDB successful")
                origin = filename
                destination = filename.split('/', 1)[1]
                move_temp_file(origin, destination)
            else:
                print("Weather - Failed to write to MongDB")


def precipitation_extract_and_load(path):
    import datetime
    filenames = get_s3_temp_filenames(path)
    if filenames == False:
        print("Precipitation - No temp data")
    else:
        for filename in filenames:
            data = get_data_from_s3(filename)
            if data == None:
                pass
            else:
                updated_time = data['cwbopendata']['location'][0]['time']['obsTime']
                updated_time = datetime.datetime.strptime(updated_time,"%Y-%m-%dT%H:%M:%S+08:00").strftime('%Y-%m-%d %H:%M:%S')
            data = {"created_at": updated_time, "item": data, "filename": filename}
            source = "precipitation"
            status = insert_s3_temp_to_mongo(source, data)
            if status == True:
                print("Precipitation - Insert to MongDB successful")
                origin = filename
                destination = filename.split('/', 1)[1]
                move_temp_file(origin, destination)
            else:
                print("Precipitation - Failed to write to MongDB")





if __name__ == '__main__':
    thread_1 = threading.Thread(target=taipei_extract_and_load, args = (TAIPEI_TEMP,))
    thread_2 = threading.Thread(target=taichung_extract_and_load, args = (TAICHUNG_TEMP,))
    thread_3 = threading.Thread(target=weather_extract_and_load, args = (WEATHER_TEMP,))
    thread_4 = threading.Thread(target=precipitation_extract_and_load, args = (PRECIPITATION_TEMP,))

    thread_1.start()
    thread_2.start()
    thread_3.start()
    thread_4.start()

    thread_1.join()
    thread_2.join()
    thread_3.join()
    thread_4.join()


