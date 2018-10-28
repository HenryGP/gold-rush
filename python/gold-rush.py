import pymongo

MONGO_URI = ""
OBJECTIVE = "SAN FRANCISCO"
OBJECTIVE_STATE = "CA"
OBJ_COORDINATES = [-122.43, 37.77]  # long, lat
INCREMENT = 50
ITERATIONS = 10


def move_location(coordinates):
    def move_point(point, objective):
        diff = abs(abs(point) - abs(objective))
        increment = INCREMENT if diff > INCREMENT else diff
        if point < objective:
            point += increment
        else:
            point -= increment
        return point

    if coordinates == OBJ_COORDINATES:
        return coordinates
    else:
        longitude = move_point(coordinates[0], OBJ_COORDINATES[0])
        latitude = move_point(coordinates[1], OBJ_COORDINATES[1])
        return [longitude, latitude]


def main():
    client = pymongo.MongoClient(MONGO_URI)
    database = client.usa
    collection = database.zips

    for iteration in range(0, ITERATIONS):
        print("Iteration number %d" % iteration)
        cities = collection.find({"city": {"$not": {"$eq": OBJECTIVE}}})
        if cities.count() == 0:
            break
        for location in cities:
            new_location = move_location(location["loc"])
            location["loc"] = new_location
            if new_location == OBJ_COORDINATES:
                location["city"] = OBJECTIVE
                location["state"] = OBJECTIVE_STATE
            print(location)
            collection.update({"_id": location["_id"]}, location)


if __name__ == "__main__":
    main()
