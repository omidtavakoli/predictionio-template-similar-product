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
movies_collection = db.new_movies
pio_users_input_collection = db.pio_input
pio_movies_input_collection = db.pio_input
pio__input_collection = db.pio_input

data = {}
data['events'] = []

def append_record(record):
    with open('event.json', 'a') as f:
        json.dump(record, f)
        f.write(os.linesep)

def import_events(output):
    count = 0
    user_count = 0
    movie_count = 0
    interacts_threshold = 15

    print("Importing data...")

    # query for all distinct user ids
    user_ids = interacts_collection.find().distinct("userid")
    for user_id in user_ids:
        print("Set user %d from %d" % (user_count, len(user_ids)))
        try:
            data['events'].append({
                'event': '$set',
                'entityType': 'user',
                'entityId': user_id
            })
            # append_record({
            #     'event': '$set',
            #     'entityType': 'user',
            #     'entityId': user_id
            # })
        except:
            print("Error")
        user_count += 1

    # getting all categories
    items = movies_collection.distinct("_source.categories")
    all_categories = []
    for category in items:
        all_categories.append(category['label'])

    # # quey for all distinct movie ids
    items = movies_collection.distinct("_id")

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
                    this_movie_category.append(all_categories.index(item['label'])+1)

            except KeyError:
                print('not category for movie ', item_id)
        print("Set item %d from %d" % (movie_count, len(item_ids)))
        data['events'].append({
            'event': '$set',
            'entityType': 'item',
            'entityId': item_id,
            'categories': this_movie_category if len(this_movie_category) > 0 else [0]
        })
        # append_record({
        #     'event': '$set',
        #     'entityType': 'item',
        #     'entityId': item_id,
        #     'categories': this_movie_category if len(this_movie_category) > 0 else [0]
        # })
        movie_count += 1

    # add users interaction events
    interactions = interacts_collection.find().limit(interacts_threshold)
    for inter in interactions:
        print("Set interactions %d from %d. User %d views item %d" % (count, interacts_threshold, inter['userid'], inter['movie_id']))
        if(inter['duration'] > 0):
            if(inter['last_watch_position'] / inter['duration'] > 0.5):

                data['events'].append({
                    'event': 'view',
                    'entityType': 'user',
                    'entityId': inter['userid'],
                    'targetEntityType': 'item',
                    'targetEntityId': inter['movie_id']
                })

                # append_record({
                #     'event': 'view',
                #     'entityType': 'user',
                #     'entityId': inter['userid'],
                #     'targetEntityType': 'item',
                #     'targetEntityId': inter['movie_id']
                # })
                count += 1
    with open(output, 'w') as outfile:
        for document in data['events']:
            outfile.write(json_util.dumps(document) + '\n')
    print("All users:%d, All Movies:%d, All Events: . Engine trained with %s events are imported with %s users and %s movies." % (len(user_ids), len(item_ids), count, user_count, movie_count))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Import sample data for similar product engine")
    # parser.add_argument('--access_key', default='invald_access_key')
    # parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--output', default="events.json")
    args = parser.parse_args()
    print(args)

    import_events(args.output)