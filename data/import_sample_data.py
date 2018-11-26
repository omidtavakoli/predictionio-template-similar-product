"""
Import sample data for similar product engine
"""

import predictionio
import argparse
import random
from pymongo import MongoClient

SEED = 3

mongo_client = MongoClient('els9.saba-e.com', 27028)
db = mongo_client.recom
interacts_collection = db.interacts
movies_collection = db.movies

def import_events(client):
  random.seed(SEED)
  count = 0
  print(client.get_status())
  print("Importing data...")

# query for all distinct user ids
  user_ids = interacts_collection.find().distinct("userid")
  for user_id in user_ids:
    print("Set user", user_id)
    client.create_event(
      event="$set",
      entity_type="user",
      entity_id=user_id
    )
    count += 1
    # quey for all distinct movie ids
  items = movies_collection.find().distinct("_id")
  item_ids = []
  for x in items:
      try:
          item_ids.append(int(x))
      except:
        pass
        print(x, " is not int")
   
  items = movies_collection.find().distinct("_source.categories")
  categories = []
  for category in items:
        categories.append(category['label'])

  for item_id in item_ids:
    print("Set item", item_id)
    client.create_event(
      event="$set",
      entity_type="item",
      entity_id=item_id,
      properties={
        "categories" : random.sample(categories, random.randint(1, 4))
      }
    )
    count += 1

  # each user randomly viewed 10 items
  for inter in interacts_collection.find().limit(1000000):
    print("User", inter['userid'] ,"views item", inter['movie_id'])
    client.create_event(
      event="view",
      entity_type="user",
      entity_id=inter['userid'],
      target_entity_type="item",
      target_entity_id= inter['movie_id']
    )
    count += 1

  print("%s events are imported." % count)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for similar product engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")

  args = parser.parse_args()
  print(args)

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client)



