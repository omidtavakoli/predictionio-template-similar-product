"""
Import sample data for similar product engine
"""

import predictionio
import argparse
import random

SEED = 3

def import_events(client):
  random.seed(SEED)
  count = 0
  print(client.get_status())
  print("Importing data...")

  user_ids = [6540651,6540654,6540665,6540689,6540691,6540703,6540706,6540707,6540715,6540722]
  # generate 10 users, with user ids u1,u2,....,u10
#   user_ids = ["u%s" % i for i in users]
  for user_id in user_ids:
    print("Set user", user_id)
    client.create_event(
      event="$set",
      entity_type="user",
      entity_id=user_id
    )
    count += 1

# { "_id" : ObjectId("5bf2cd125024160132bb5768"), "id" : 135533359, "userid" : 6540651, "movie_id" : 15010, "last_watch_position" : 3515, "duration" : 3660, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "online" }
# { "_id" : ObjectId("5bf3d1f05359112f2b3ba70b"), "id" : 135533359, "userid" : 6540651, "movie_id" : 15010, "last_watch_position" : 3515, "duration" : 3660, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "online" }
# { "_id" : ObjectId("5bf2cd125024160132bb59d3"), "id" : 135534011, "userid" : 6540654, "movie_id" : 15418, "last_watch_position" : 2744, "duration" : 2640, "sdate" : "8/9/2018", "visit_type" : "site", "stream_type" : "online" }
# { "_id" : ObjectId("5bf3d1f05359112f2b3ba974"), "id" : 135534011, "userid" : 6540654, "movie_id" : 15418, "last_watch_position" : 2744, "duration" : 2640, "sdate" : "8/9/2018", "visit_type" : "site", "stream_type" : "online" }
# { "_id" : ObjectId("5bf2cd125024160132bb5dec"), "id" : 135535078, "userid" : 6540665, "movie_id" : 5911, "last_watch_position" : 377, "duration" : 360, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "online" }
# { "_id" : ObjectId("5bf2cd125024160132bb62bf"), "id" : 135536359, "userid" : 6540665, "movie_id" : 9748, "last_watch_position" : 2254, "duration" : 1440, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "online" }
# { "_id" : ObjectId("5bf3d1f05359112f2b3bad8c"), "id" : 135535078, "userid" : 6540665, "movie_id" : 5911, "last_watch_position" : 377, "duration" : 360, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "online" }
# { "_id" : ObjectId("5bf3d1f15359112f2b3bb278"), "id" : 135536359, "userid" : 6540665, "movie_id" : 9748, "last_watch_position" : 2254, "duration" : 1440, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "online" }
# { "_id" : ObjectId("5bf2cd125024160132bb5ed4"), "id" : 135535314, "userid" : 6540689, "movie_id" : 15559, "last_watch_position" : 0, "duration" : 3073, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "offline" }
# { "_id" : ObjectId("5bf3d1f05359112f2b3bae79"), "id" : 135535314, "userid" : 6540689, "movie_id" : 15559, "last_watch_position" : 0, "duration" : 3073, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "offline" }
# { "_id" : ObjectId("5bf2cd125024160132bb626b"), "id" : 135536273, "userid" : 6540691, "movie_id" : 4789, "last_watch_position" : 464, "duration" : 420, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "online" }
# { "_id" : ObjectId("5bf3d1f15359112f2b3bb20e"), "id" : 135536273, "userid" : 6540691, "movie_id" : 4789, "last_watch_position" : 464, "duration" : 420, "sdate" : "8/9/2018", "visit_type" : "android", "stream_type" : "online" }
# { "_id" : ObjectId("5bf2cd125024160132bb6632"), "id" : 135537265, "userid" : 6540703, "movie_id" : 15338, "last_watch_position" : 2863, "duration" : 3000, "sdate" : "8/9/2018", "visit_type" : "site", "stream_type" : "online" }
# { "_id" : ObjectId("5bf2cd135024160132bb900f"), "id" : 135548235, "userid" : 6540703, "movie_id" : 15559, "last_watch_position" : 1586, "duration" : 1320, "sdate" : "8/9/2018", "visit_type" : "site", "stream_type" : "online" }
# { "_id" : ObjectId("5bf3d1f15359112f2b3bb5d7"), "id" : 135537265, "userid" : 6540703, "movie_id" : 15338, "last_watch_position" : 2863, "duration" : 3000, "sdate" : "8/9/2018", "visit_type" : "site", "stream_type" : "online" }
# { "_id" : ObjectId("5bf3d1f25359112f2b3bdfb2"), "id" : 135548235, "userid" : 6540703, "movie_id" : 15559, "last_watch_position" : 1586, "duration" : 1320, "sdate" : "8/9/2018", "visit_type" : "site", "stream_type" : "online" }
# { "_id" : ObjectId("5bf2cd125024160132bb6a5c"), "id" : 135538353, "userid" : 6540706, "movie_id" : 5984, "last_watch_position" : 2681, "duration" : 2460, "sdate" : "8/9/2018", "visit_type" : "site", "stream_type" : "online" }
# { "_id" : ObjectId("5bf3d1f15359112f2b3bba05"), "id" : 135538353, "userid" : 6540706, "movie_id" : 5984, "last_watch_position" : 2681, "duration" : 2460, "sdate" : "8/9/2018", "visit_type" : "site", "stream_type" : "online" }
# { "_id" : ObjectId("5bf2cd125024160132bb641f"), "id" : 135536726, "userid" : 6540707, "movie_id" : 15641, "last_watch_position" : 0, "duration" : 2989, "sdate" : "8/9/2018", "visit_type" : "ios", "stream_type" : "offline" }
# { "_id" : ObjectId("5bf2cd125024160132bb66f8"), "id" : 135537462, "userid" : 6540707, "movie_id" : 15338, "last_watch_position" : 0, "duration" : 3165, "sdate" : "8/9/2018", "visit_type" : "ios", "stream_type" : "offline" }


  item_ids = [15010,15418,5911,9748,15559,4789,15338,5984,15641,15338]
  categories = ["خانوادگی", "اجتماعی", "علمی - تخیلی", "عاشقانه", "دوبله ترکی"]
  # generate 50 items, with item ids i1,i2,....,i50
  # random assign 1 to 4 categories among c1-c6 to items
#   categories = ["c%s" % i for i in range(1, 7)]
#   item_ids = ["i%s" % i for i in range(1, 51)]
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
  for user_id in user_ids:
    for viewed_item in random.sample(item_ids, 10):
      print("User", user_id ,"views item", viewed_item)
      client.create_event(
        event="view",
        entity_type="user",
        entity_id=user_id,
        target_entity_type="item",
        target_entity_id=viewed_item
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
