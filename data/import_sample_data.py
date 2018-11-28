"""
Import sample data for similar product engine
"""

import predictionio
import argparse
import random
from pymongo import MongoClient
import operator

SEED = 3

mongo_client = MongoClient('els9.saba-e.com', 27028)
db = mongo_client.recom
interacts_collection = db.interacts
movies_collection = db.movies

def import_events(client):
  random.seed(SEED)
  count = 0
  user_count = 0
  movie_count = 0

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
    user_count += 1
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
      this_movie_category = []
      for movie in movies_collection.find({"_id": "2454"}, {"_source.categories": 1 }):
        for item in movie['_source']['categories']:
          this_movie_category.append(item['label'])
      
    print("Set item", item_id)
    client.create_event(
      event="$set",
      entity_type="item",
      entity_id=item_id,
      properties={
        "categories" : categories
      }
    )
    movie_count += 1

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

  print("%s events are imported with %s users and %s movies." % (count, user_count, movie_count))

if __name__ == '__main__':
  # main_movie_users = interacts_collection.find({"movie_id": 2822}).distinct("userid")
  # temp = interacts_collection.find({"movie_id": 2454}).distinct("userid")
  # print(len(set(main_movie_users) & set(temp)))    # print(temp))
  # stat[movie] = len(list(set(main_movie_users) & set(temp)))
  # all_movies = movies_collection.find().distinct("_id")
  # all_movies = [6659, 8086, 14286, 9270, 7053, 8881, 8999, 12429, 5881, 13076, 6925, 1123, 5359, 11967, 10618, 13313, 12096, 6173, 473, 6218, 19, 6176, 9133, 13002, 3558, 4440, 4445, 4450, 4452, 10916, 11390, 9596, 2334, 7199, 12646, 2665, 656, 13700, 14555, 6175, 8972, 9983, 8187, 13263, 8304, 278, 2843, 6889, 6436, 10432]
  # print(len(all_movies))
  # stat = {}
  # index = 0
  # for movie in all_movies:
  #   index +=1
  #   if index > 150: break
  #   # print(int(movie))
  #   temp = interacts_collection.find({"movie_id": int(movie)}).distinct("userid")
  #   # print(temp)
  #   stat[movie] = len(list(set(main_movie_users) & set(temp)))
  #   print(movie)
  #   print(set(main_movie_users) & set(temp))
  # sorted_stat = sorted(stat.items(), key=operator.itemgetter(1))
  # print(sorted_stat)
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



