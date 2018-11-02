package com.goldrush;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.lang.Math;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.not;

public class GoldRush {

    private final String MONGO_URI = "";
    private final String OBJECTIVE = "SAN FRANCISCO";
    private final String OBJECTIVE_STATE = "CA";
    private final double[] OBJ_COORDINATES = { -122.43, 37.77 }; // long, lat
    private final int INCREMENT = 50;
    private final int ITERATIONS = 10;

    private double movePoint(double point, double objective) {
        double diff = Math.abs(Math.abs(point) - Math.abs(objective));
        double increment = diff > this.INCREMENT ? this.INCREMENT : diff;
        if (point < objective)
            point += increment;
        else
            point -= increment;
        return point;
    }

    private double[] moveLocation(double[] coordinates) {
        if (Arrays.equals(coordinates, this.OBJ_COORDINATES)) {
            return coordinates;
        } else {
            double longitude = this.movePoint(coordinates[0], this.OBJ_COORDINATES[0]);
            double latitude = this.movePoint(coordinates[1], this.OBJ_COORDINATES[1]);
            double[] new_location = { longitude, latitude };
            return new_location;
        }
    }

    public void processData() {
        MongoClient client = new MongoClient(new MongoClientURI(this.MONGO_URI));

        MongoDatabase database = client.getDatabase("usa");
        MongoCollection<Document> collection = database.getCollection("zips");

        for (int iteration = 0; iteration <= this.ITERATIONS; iteration++) {
            System.out.println("Iteration number " + iteration);

            MongoCursor<Document> cities = collection.find(not(eq("city", "SAN FRANCISCO"))).iterator();

            try {
                if (!cities.hasNext())
                    break;
                while (cities.hasNext()) {

                    Document city = cities.next();
                    ArrayList location = (ArrayList) city.get("loc");

                    double[] current_location = { (double) location.get(0), (double) location.get(1) };
                    double[] new_location = this.moveLocation(current_location);

                    if (Arrays.equals(new_location, this.OBJ_COORDINATES)) {
                        city.put("city", this.OBJECTIVE);
                        city.put("state", this.OBJECTIVE_STATE);
                    }

                    List<Double> array = new ArrayList<Double>();
                    array.add(new_location[0]);
                    array.add(new_location[1]);
                    city.put("loc", array);

                    System.out.println(city.toJson());
                    collection.replaceOne(eq("_id", city.get("_id")), city);
                }
            } finally {
                cities.close();
            }
        }
        // release resources
        client.close();
    }

    public static void main(final String[] args) {
        GoldRush goldRush = new GoldRush();
        goldRush.processData();
    }
}
