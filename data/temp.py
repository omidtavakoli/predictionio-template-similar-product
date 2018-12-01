    items = movies_collection.find().distinct("_id")
    all_movies = []
    for x in items:
        try:
            all_movies.append(int(x))
        except:
            print(x, " is not int")
            pass
    # print(len(numbers))
    all_movies_in_interacts = interacts_collection.find().distinct("movie_id")
    print(len(all_movies_in_interacts))
    # print(set(numbers) & set(temp))
    x = [item for item in all_movies if item not in all_movies_in_interacts]
    print(len(all_movies))
    print(x)