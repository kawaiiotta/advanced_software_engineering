# this code will get history data of twitter which will take longer

# import twitter scraper
from twitt_live import get_twitter_data

# import math and time
import pandas as pd
from datetime import date, timedelta
import time
import datetime as datetime

# import database
from mongo_connection import get_database


class twitter_history():
    def __init__(self, query, timeperiod):
        self.query = [query]
        # get dates in year month day format
        self.today = date.today()
        self.yesterday = self.today - timedelta(days=1)
        self.history_time = timeperiod


    def history(self):
        while True:
            print("STARTING TO GET THE DATA")
            start = time.time()

            # for every query run the scraper
            for query in self.query:
                # request
                get_twitter_data(query, self.yesterday, self.today)

            self.yesterday -= timedelta(days=1)
            self.today -= timedelta(days=1)

            print(time.time() - start)

            # run the code immediately and break whenever the day is above 1 month
            if date.today() - timedelta(days=self.history_time) == self.today:
                break



if __name__ == "__main__":

    print("--------------------------------------------")
    queries = ["bitcoin", "etherium", "crypto", "elonmusk", "XRP", "blockchain", "NFT"]
    print(queries)