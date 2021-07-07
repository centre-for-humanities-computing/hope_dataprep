"""
This script run the preprocessing / lang extract once a week

"""
from datetime import date
import os
import time

is_run = False
while True:
    time.sleep(60*60)  # wait one hour
    if date.today().weekday() == 1:  # monday = 0, tuesday = 1 ...
        if is_run is False:
            is_run = True
            os.system('python3 extract_nordic_tweets.py')
        else:
            pass
    else:
        is_run = False
