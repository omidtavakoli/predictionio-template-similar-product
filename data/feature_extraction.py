import argparse
import random
from pymongo import MongoClient
import json
import newlinejson as nlj
from bson import json_util

SEED = 3

mongo_client = MongoClient('els9.saba-e.com', 27028)
db = mongo_client.recom
interacts_collection = db.interacts
movies_collection = db.movies
data = {}
data['events'] = []


def import_events():
    random.seed(SEED)
    count = 0
    user_count = 0
    movie_count = 0

    print("Importing data...")

    # query for all distinct user ids
    user_ids = interacts_collection.find().distinct("userid")
    for user_id in user_ids:
        print("Set user", user_id)
        data['events'].append({
            'event': '$set',
            'entityType': 'user',
            'entityId': user_id
        })
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
                print('not category for movie ', item_id)
        print("Set item", item_id)
        data['events'].append({
            'event': '$set',
            'entityType': 'item',
            'entityId': item_id,
            'categories': this_movie_category
        })
        movie_count += 1

    # add users interaction events
    for inter in interacts_collection.find().limit(5000000):
        print("User", inter['userid'], "views item", inter['movie_id'])

        data['events'].append({
            'event': 'view',
            'entityType': 'user',
            'entityId': inter['userid'],
            'targetEntityType': 'item',
            'targetEntityId': inter['movie_id']
        })

        count += 1

    with open('events.json', 'w') as outfile:
        for document in data['events']:
            outfile.write(json_util.dumps(document) + '\n')

    print("%s events are imported with %s users and %s movies." % (count, user_count, movie_count))


if __name__ == '__main__':
    import_events()





