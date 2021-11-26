package flights;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Iterator;

public class FlightApp {
    private static Function2 handlingCVS  = new Function2<Integer, Iterator<String>, Iterator<String>>() {
        public Iterator<String> call(Integer index, Iterator<String> iter) {
            if (index == 0 && iter.hasNext()) {
                iter.next();
                return iter;
            }
            return iter;
        }
    };

    private mapFlights = new Tuple2<Integer, String>(String str) {

    }

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

        JavaRDD<String> flightRddRecords = sctx.textFile(flightMapperPath).
                mapPartitionsWithIndex(handlingCVS, false);
        JavaRDD<String> airportRddRecords = sctx.textFile(airportMapperPath).
                mapPartitionsWithIndex(handlingCVS, false);;

        JavaPairRDD<Integer, String> flightRddPairs = flightRddRecords.mapToPair(
                x -> mapFlights(x)
        );
        JavaPairRDD<Integer, String> airportRddPairs = airportRddRecords.mapToPair();

        final Broadcast<Map<String, AirportData>> airportsBroadcasted = sctx.broadcast(stringAirportDataMap);
    }
}
