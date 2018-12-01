import argparse
import random
from pymongo import MongoClient
import json
import newlinejson as nlj
from bson import json_util
import os


mongo_client = MongoClient('els9.saba-e.com', 27028)
db = mongo_client.recom
interacts_collection = db.interacts
movies_collection = db.movies
pio_users_input_collection = db.pio_input
pio_movies_input_collection = db.pio_input
pio__input_collection = db.pio_input

data = {}
data['events'] = []
not_catgory = []

f= open("events.json","w+")

def append_record(record):
    with open('temp.json', 'a') as f:
        json.dump(record, f)
        f.write(os.linesep)

def import_events(output):
    count = 0
    user_count = 0
    movie_count = 0
    interacts_threshold = 10000000

    print("Importing data...")

    # query for all distinct user ids
    user_ids = interacts_collection.find().distinct("userid")
    for user_id in user_ids:
        print("Set user", user_id)
        try:
            append_record({
                'event': '$set',
                'entityType': 'user',
                'entityId': user_id
            })
        except:
            print("Error")
        user_count += 1

    # getting all categories
    items = movies_collection.find().distinct("_source.categories")
    all_categories = []
    for category in items:
        all_categories.append(category['label'])

    # quey for all distinct movie ids
    items = movies_collection.find().distinct("_id")

    item_ids = []
    for x in items:
        try:
            item_ids.append(int(x))
        except:
            print(x, " is not int")
            pass

    for item_id in item_ids:
        this_movie_category = []
        for movie in movies_collection.find({"_id": str(item_id)}, {"_source.categories": 1}):
            try:
                for item in movie['_source']['categories']:
                    this_movie_category.append(all_categories.index(item['label']))

            except KeyError:
                # not_catgory.append(item_id)
                print('not category for movie ', item_id)
        if(len(this_movie_category) > 0):       
            print("Set item", item_id)
            append_record({
                'event': '$set',
                'entityType': 'item',
                'entityId': item_id,
                'categories': this_movie_category
            })
            movie_count += 1
        else:
            print("movie %d removed due to not categorized", item_id)

    # add users interaction events
    for inter in interacts_collection.find().limit(interacts_threshold):
        print("User", inter['userid'], "views item", inter['movie_id'])

        append_record({
            'event': 'view',
            'entityType': 'user',
            'entityId': inter['userid'],
            'targetEntityType': 'item',
            'targetEntityId': inter['movie_id']
        })

        count += 1

    print("All users:%d, All Movies:%d, All Events: . Engine trained with %s events are imported with %s users and %s movies." % (len(user_ids), len(item_ids), count, user_count, movie_count))
    # print(not_catgory)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Import sample data for similar product engine")
    # parser.add_argument('--access_key', default='invald_access_key')
    # parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--output', default="events.json")
    args = parser.parse_args()
    print(args)

    import_events(args.output)






