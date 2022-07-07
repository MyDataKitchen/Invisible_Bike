import schedule
import subprocess
import time

def do_taipei_youbike_crawling():
    subprocess.run('python3 taipei_youbike_crawler.py', shell=True, cwd='crawlers')

def do_taichung_youbike_crawling():
    subprocess.run('python3 taichung_youbike_crawler.py', shell=True, cwd='crawlers')

def do_precipitation_crawling():
    subprocess.run('python3 precipitation_crawler.py', shell=True, cwd='crawlers')

def do_wheather_crawling():
    subprocess.run('python3 weather_crawler.py', shell=True, cwd='crawlers')

if __name__ == '__main__':

    schedule.every(1).minutes.do(do_taipei_youbike_crawling)
    schedule.every(1).minutes.do(do_taichung_youbike_crawling)
    schedule.every(10).minutes.do(do_precipitation_crawling)
    schedule.every(10).minutes.do(do_wheather_crawling)

    while True:
        schedule.run_pending()
        time.sleep(1)