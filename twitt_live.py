# this code will try to get live data of all tweets that have been made yesterday
# it will take the data put it in a dataframe and then save it to the database
# input will be the key word for which thing it should get the data
# things that might go wrong -> no smooth connection between data between days

# import twitter api
from pkgutil import get_data
import snscrape.modules.twitter as sntwitter

# import math and time
import pandas as pd
from datetime import date, timedelta
import time
import datetime as datetime

# import database
from mongo_connection import get_database
from twitt_queries import queries


"""
This class will request data from a specific timeframe of twitter with a specific input string
Input:  timeframe -> default: yesterday-today
        query/searchword/hashtag

Output: dataframe of tweets consisting:
        - username of tweeter
        - date of creation
        - amount of Likes
        - source Label i.e.: iPhone
        - tweet content containing searchword
"""
class get_twitter_data():

    def __init__(self, queue, yesterday=None, today=None):
        # make sure queue is always a string
        self.queue = str(queue)
        # default to today
        self.today = date.today() if today == None else today
        # default to yesterday
        self.yesterday = today - timedelta(days=1) if yesterday == None else yesterday
        # start the requesting
        self._twitter_scraper()
        # save tweets to database
        self._save_to_db()


    """
    request data from twitter and create a dataframe
    """
    def _twitter_scraper(self):

        attributes_container = [[tweet.user.username, 
                                 tweet.date, 
                                 tweet.likeCount, 
                                 tweet.sourceLabel, 
                                 tweet.content] for i,tweet in enumerate(sntwitter.TwitterSearchScraper(f'{str(self.queue)} since:{str(self.yesterday)} until:{str(self.today)}', top=True).get_items())]
        # Creating a dataframe to load the list
        print(f'{str(self.queue)} since:{str(self.yesterday)} until:{str(self.today)}')
        self.tweets_df = pd.DataFrame(attributes_container, columns=["User", "Date Created", "Number of Likes", "Source of Tweet", "Tweet"])


    """
    save df to database with name of the queue
    """
    def _save_to_db(self):

        client = get_database()

        # make sure data is not 0 len
        if len(self.tweets_df) != 0:
            json_data = self.tweets_df.to_dict("records")
            client["twitter_data"]["twitter_dump "+self.queue].insert_many(json_data)


if __name__ == "__main__":

    print("--------------------------------------------")
    q = queries()
    queries_ls = q.retrieve_query()
    # queries = ["bitcoin", "etherium", "crypto", "elonmusk", "XRP", "blockchain", "NFT"]
    # queries = ["waldbrandgefahr", "motorschaden", "deine mudda"]

    """
    task to execute after time
    """
    def job():
        print("STARTING TO GET THE DATA")
        start = time.time()

        # for every query run the scraper
        for query in queries_ls:
            # get dates in year month day format
            today = date.today()
            yesterday = today - timedelta(days=1)
            # request
            get_twitter_data(query, yesterday, today)

        print(time.time() - start)

    # run every morning at 1 am
    while True:
        curDateTime = datetime.datetime.now()
        if curDateTime.hour == 1 and curDateTime.minute <= 1:
            job()
            time.sleep(30)
