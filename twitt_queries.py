from mongo_connection import get_database


class queries():

    def __init__(self):
        self.table = get_database()["twitter_data"]["queries"]


    def add_query(self, query):
        # add it only if it doesnt exist yet
        if self.table.count_documents({'query': query}, limit = 1) == 0:
            json_data = {"query": f"{query}"}
            self.table.insert_one(json_data)

    
    def retrieve_query(self):
        ls = []
        for query in self.table.find():
            ls.append(query["query"])
        return ls


if __name__ == "__main__":
    # add default queries
    queri = ["bitcoin", "etherium", "crypto", "elonmusk", "XRP", "blockchain", "NFT"]
    
    q = queries()
    for query in queri:
        q.add_query(query)
    
    print(q.retrieve_query())