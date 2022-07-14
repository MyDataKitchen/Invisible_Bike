import subprocess

if __name__ == '__main__':
    subprocess.run('python3 mongo_to_mysql.py', shell=True, cwd='etl')