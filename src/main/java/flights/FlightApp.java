package flights;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FlightApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: FlightApp <1: input path FlightMapper> <2: input path AirportMapper> <output path>");
            System.exit(-1);
        }

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sctx = new JavaSparkContext(conf);

        String flightMapperPath = args[0];
        String airportMapperPath = args[1];
        String outPath = args[2];

        JavaRDD<String> flightRddRecords =  sctx.textFile(flightMapperPath);
        JavaRDD<String> airportRddRecords =  sctx.textFile(airportMapperPath);

        JavaPairRDD<Integer, String> flightRddPairs = flightRddRecords.mapToPair();

        final Broadcast<Map<String, AirportData>> airportsBroadcasted = sctx.broadcast(stringAirportDataMap);
    }
}
