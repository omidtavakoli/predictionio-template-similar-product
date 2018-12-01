
    # items = movies_collection.find().distinct("_id")
    # numbers = []
    # for x in items:
    #     try:
    #         numbers.append(int(x))
    #     except:
    #         print(x, " is not int")
    #         pass
    # # print(len(numbers))
    # temp = interacts_collection.find().distinct("movie_id")
    # # print(len(temp))
    # # print(set(numbers) & set(temp))
    # print([item for item in numbers if item not in temp])