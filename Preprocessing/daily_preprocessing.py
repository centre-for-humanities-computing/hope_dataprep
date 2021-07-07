"""
This script runs lang extraction every day

"""
import schedule
import time
import os

# define the job: "run the defined python script from terminal"
def job():
    os.system('python3 extract_nordic_tweets.py')

# schedule the job to run every day at 6 in the morning
schedule.every().day.at("06:00").do(job)

# run the ongoing script to check every hour if it's 6 am yet
while True:
    schedule.run_pending()
    time.sleep(1)