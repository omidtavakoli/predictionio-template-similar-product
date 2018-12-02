import argparse
import random
from pymongo import MongoClient
import json
import newlinejson as nlj
from bson import json_util
import os
import math
import sys 

mongo_client = MongoClient('els9.saba-e.com', 27028)
db = mongo_client.recom
interacts_collection = db.interacts
movies_collection = db.new_movies
pio_users_input_collection = db.pio_input
pio_movies_input_collection = db.pio_input
pio__input_collection = db.pio_input

users_data = {}
users_data['users'] = []

items_data = {}
items_data['items'] = []

interactions_data = {}
interactions_data['interacts'] = []

interacts_threshold = 10000000

def append_record(record):
    with open('events.json', 'a') as f:
        json.dump(record, f)
        f.write(os.linesep)

def import_users(output):
    user_count = 0
    print("Importing users data...")
    # query for all distinct user ids
    user_ids = interacts_collection.find().distinct("userid")
    for user_id in user_ids:
        print("%d user imported from %d." % (user_count, len(user_ids)))
        try:
            users_data['users'].append({
                'event': '$set',
                'entityType': 'user',
                'entityId': user_id
            })
        except:
            print("Error")
        user_count += 1
    
    with open(output, 'w') as outfile:
        for document in users_data['users']:
            outfile.write(json_util.dumps(document) + '\n')
    print("%d users imported from %d." % (len(user_ids), user_count))

def import_items(output):
    movie_count = 0
    print("Importing items data...")

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
        items_data['items'].append({
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

    with open(output, 'w') as outfile:
        for document in items_data['items']:
            outfile.write(json_util.dumps(document) + '\n')
    print("%d items imported." % (movie_count))


def import_interactions(output):
    count = 0
    print("Importing interactions data...")
    # add users interaction events
    interactions = interacts_collection.find().limit(interacts_threshold)
    for inter in interactions:
        print("Set interactions %d from %d. User %d views item %d" % (count, interacts_threshold, inter['userid'], inter['movie_id']))
        if(inter['duration'] > 0):
            # index  = math.ceil((inter['last_watch_position'] / inter['duration']) / 0.5)
            if(inter['last_watch_position'] / inter['duration'] < 0.5):
                interactions_data['interacts'].append({
                    'event': 'view',
                    'entityType': 'user',
                    'entityId': inter['userid'],
                    'targetEntityType': 'item',
                    'targetEntityId': inter['movie_id']
                })
            else:
                interactions_data['interacts'].append({
                    'event': 'view',
                    'entityType': 'user',
                    'entityId': inter['userid'],
                    'targetEntityType': 'item',
                    'targetEntityId': inter['movie_id']
                })
                interactions_data['interacts'].append({
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
        for document in interactions_data['interacts']:
            outfile.write(json_util.dumps(document) + '\n')
    print("%d interactions imported." % (count))

def appending():

    users = open("users.json", "r")
    users_data = users.read()
    users.close()

    items = open("items.json", "r")
    items_data = items.read()
    items.close()

    interactions = open("interactions.json", "r")
    interactions_data = interactions.read()
    interactions.close()

    events = open("events.json", "a")
    events.write(users_data+ "\n")
    events.write(items_data+ "\n")
    events.write(interactions_data+ "\n")
    events.close()

    print("merging done to events.json file.")


if __name__ == '__main__':
    # parser = argparse.ArgumentParser(
        # description="Import sample data for similar product engine")
    # parser.add_argument('--access_key', default='invald_access_key')
    # parser.add_argument('--url', default="http://localhost:7070")
    # parser.add_argument('--output', default="events.json")
    # args = parser.parse_args()
    # print(args)
    print(sys.argv)
    if 'users' in sys.argv: import_users("users.json")
    if 'items' in sys.argv: import_items("items.json")
    if 'interactions' in sys.argv: import_interactions("interactions.json")
    if 'merge' in sys.argv: appending()