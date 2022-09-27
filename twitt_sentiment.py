from sentiment_analyser import classify_this
from mongo_connection import get_database
from twitt_queries import queries
import time
from bson.objectid import ObjectId
import pymongo
from pymongo.errors import ServerSelectionTimeoutError, CursorNotFound

if __name__ == "__main__":
    # initialize classifier and train it.
    # this will take up to 5 mins
    classifier = classify_this()

    while True:
        print("looking for data")
        # connect to the database and check what needs to be classified

        # database object
        client = get_database()
        q = queries()
        # for every query 
        try:    
            for query in q.retrieve_query():
                # get table
                print(query)
                table = client["twitter_data"][f"twitter_dump {query}"]
                # loop through table
                try:
                    for tweet in table.find():
                        # calculate incentive
                        sentiment = classifier.get_incentive(str(tweet["Tweet"]))
                        simple_score = classifier.return_simple_score(str(tweet["Tweet"]))
                        
                        # put into new table database
                        tweet["sentiment"] = sentiment
                        tweet["score"] = simple_score

                        try:
                            # add to new sentiment table
                            json_data = tweet
                            client["twitter_data"][f"twitter_sentiment {query}"].insert_one(json_data)
                            print(tweet["_id"])
                        except pymongo.errors.DuplicateKeyError:
                            print(tweet["_id"])
                        finally:
                            # delete entry by id
                            remove = {"_id": ObjectId(tweet["_id"])}
                            table.delete_one(remove)

                except CursorNotFound:
                    print("CURSOR NOT FOUND")
                    time.sleep(30)
                    continue        # continue to loop through the queries but stop looping through the tweets 
                    
        except ServerSelectionTimeoutError:
            print("SERVERTIMEOUT")
            time.sleep(120)   


        # check again if new entries exist
        time.sleep(60)
