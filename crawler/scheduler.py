import schedule
import subprocess
import time
import threading

def taipei_youbike():
    subprocess.run('python3 taipei_youbike_crawler.py', shell=True, cwd='crawlers')

def taichung_youbike():
    subprocess.run('python3 taichung_youbike_crawler.py', shell=True, cwd='crawlers')

def precipitation():
    subprocess.run('python3 precipitation_crawler.py', shell=True, cwd='crawlers')

def wheather():
    subprocess.run('python3 weather_crawler.py', shell=True, cwd='crawlers')

def do_youbike_crawling():
    thread_1 = threading.Thread(target=taipei_youbike)
    thread_2 = threading.Thread(target=taichung_youbike)
    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()

def do_precipitation_and_wheather_crawling():
    thread_1 = threading.Thread(target=precipitation)
    thread_2 = threading.Thread(target=wheather)
    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()

if __name__ == '__main__':

    schedule.every(1).minutes.do(do_youbike_crawling)
    schedule.every(10).minutes.do(do_precipitation_and_wheather_crawling)

    while True:
        schedule.run_pending()
        time.sleep(1)