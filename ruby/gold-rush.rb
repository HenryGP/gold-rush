require 'mongo'

MONGO_URI = ""
OBJECTIVE = "SAN FRANCISCO"
OBJECTIVE_STATE = "CA"
OBJ_COORDINATES = [-122.43, 37.77]  # long, lat
INCREMENT = 50
ITERATIONS = 10


def move_point(point, objective)
	diff = (point.abs - objective.abs).abs
	increment = diff > INCREMENT ? INCREMENT : diff
	if point < objective then
		point += increment
	else
		point -= increment
	end
	return point
end


def move_location(coordinates)
	if coordinates == OBJ_COORDINATES then
		return coordinates		
	else
		longitude = move_point(coordinates[0], OBJ_COORDINATES[0])
		latitude = move_point(coordinates[1], OBJ_COORDINATES[1])
		return [longitude, latitude]
	end
end


def main()
	client = Mongo::Client.new(MONGO_URI, :database => 'usa')
	collection = client[:zips]

	(1..ITERATIONS).each do |iteration|
		puts "Iteration number #{iteration}"
		cities = collection.find({:city => {:$not => {:$eq => OBJECTIVE}}})
		if cities.count() == 0 then
			break
		end
		cities.each do |location| 
			new_location = move_location(location[:loc])
			location[:loc] = new_location
			if new_location == OBJ_COORDINATES then
				location[:city] = OBJECTIVE
				location[:state] = OBJECTIVE_STATE 
			end
			puts location
			collection.update_one({:_id => location[:_id]}, location)
		end
	end
	client.close
end


if __FILE__ == $0
    main()
end