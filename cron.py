import schedule
import time
import subprocess

script1_path = "C:\\Users\\Cars24\\Desktop\\cars24\\Logistics.py"
# script2_path = "/path/to/your/python/script2.py"

def job1():
    subprocess.run(["python", script1_path])


schedule.every().hour.do(job1)
while True:
    schedule.run_pending()
    time.sleep(1)
    
