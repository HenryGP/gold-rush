using System;
using System.Linq;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Driver;

namespace tour
{
    class GoldRush
    {
        private String MONGO_URI = "";
        private String OBJECTIVE = "SAN FRANCISCO";
        private String OBJECTIVE_STATE = "CA";
        private double[] OBJ_COORDINATES = new double[] { -122.43, 37.77 };  // long, lat
        private double INCREMENT = 50;
        private int ITERATIONS = 10;

        private double MovePoint(double point, double objective)
        {
            var diff = Math.Abs(Math.Abs(point) - Math.Abs(objective));
            double increment = diff > this.INCREMENT ? this.INCREMENT : diff;
            if (point < objective)
            {
                point += increment;
            }
            else
            {
                point -= increment;
            }
            return point;
        }

        private double[] MoveLocation(double[] coordinates)
        {
            if (new HashSet<double>(coordinates).SetEquals(this.OBJ_COORDINATES))
            {
                return coordinates;
            }
            else
            {
                double longitude = this.MovePoint(coordinates[0], this.OBJ_COORDINATES[0]);
                double latitude = this.MovePoint(coordinates[1], this.OBJ_COORDINATES[1]);
                double[] new_location = new double[] { longitude, latitude };
                return new_location;
            }
        }

        private void processData()
        {
            var client = new MongoClient(new MongoUrl(MONGO_URI));
            var database = client.GetDatabase("usa");
            var collection = database.GetCollection<BsonDocument>("zips");

            var builder = Builders<BsonDocument>.Filter;
            var filter = builder.Not(builder.Eq("city", "SAN FRANCISCO"));

            for (int iteration = 0; iteration < this.ITERATIONS; iteration++)
            {
                Console.WriteLine("Iteration number {0}", iteration);

                using (var cities = collection.Find(filter).ToCursor())
                {
                    while (cities.MoveNext())
                    {
                        foreach (var doc in cities.Current)
                        {
                            Console.WriteLine(doc);
                            double[] arrayOfDouble = doc.GetValue("loc").AsBsonArray.Select(p => p.AsDouble).ToArray();
                            var new_location = this.MoveLocation(arrayOfDouble);
                            doc.Remove("loc");
                            doc.Add("loc", BsonValue.Create(new_location));

                            if (new HashSet<double>(new_location).SetEquals(this.OBJ_COORDINATES))
                            {
                                doc.Remove("city");
                                doc.Add("city", BsonValue.Create(this.OBJECTIVE));
                                doc.Remove("state");
                                doc.Add("state", BsonValue.Create(this.OBJECTIVE_STATE));
                            }
                            Console.WriteLine(doc);

                            collection.ReplaceOne(builder.Eq("_id", doc.GetValue("_id").ToString()), doc);
                        }
                    }
                }
            }
        }

        static void Main(string[] args)
        {
            GoldRush goldRush = new GoldRush();
            goldRush.processData();
        }
    }
}

