from pymongo import MongoClient
import pymongo


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    user = "mustafaplzbookroom"
    pw = "whyisntwebsitedone42"

    CONNECTION_STRING = f"mongodb://{user}:{pw}@116.15.216.251:27018/?authSource=admin&readPreference=primary&ssl=false"
    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client


if __name__ == "__main__":

    client = get_database()
    print(client)
    # json_data = df.to_dict("records")
    # client["twitter_data"]["twitter_dump"].insert_many(json_data)
