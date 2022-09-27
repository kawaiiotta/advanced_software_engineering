from mongo_connection import get_database

db = get_database()

def check_query(query):
    queries= db.twitter_data['queries']
    return queries.count_documents({"query":query})

def list_query():
    queries= db.twitter_data['queries'].find()
    lst = [q.get('query') for q in queries]
    return lst

def find_query(query):
    DTFORMAT = "%d-%b-%Y"
    avg=[]
    totalsent=0
    totalscore=0
    count=0
    contents = db.twitter_data[f"twitter_sentiment {query}"].find()
    current_date= db.twitter_data[f"twitter_sentiment {query}"].find_one().get('Date Created').strftime(DTFORMAT)

    for c in contents:
        if c.get('Date Created').strftime(DTFORMAT) == current_date:
            totalscore += c.get('score')
            totalsent += c.get('sentiment')
            count += 1
        
        else:
            avg.append((totalscore/count, totalsent/count, current_date))
            count = 1
            current_date = c.get('Date Created').strftime(DTFORMAT)
            totalscore = c.get('score')
            totalsent = c.get('sentiment')

    return  avg

def add_query(query):
    mydict={'query': query}
    db.twitter_data['queries'].insert_one(mydict)

print(list_query())