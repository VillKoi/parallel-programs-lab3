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

    private static final String STRING_SPLITTER = ",";
    private static final String DOUBLE_QUOTES = "\"";
    // number in cvs
    private static final int DEST_AIRPORT_ID = 14;
    private static final int ORIGIN_AIRPORT_ID = 11;
    private static final int ARR_DELAY = 18;
    private static final int CANCELLED = 19;

    private static String removeDoubleQuotes(String value) {
        return value.replaceAll(DOUBLE_QUOTES, "");
    }

    private static boolean correctDelayingTime(float delay) {
        return delay != 0;
    }

    private static Tuple2<Tuple2<Integer, Integer>, String>  mapFlights(String text) {
        String[] values = text.split(STRING_SPLITTER);

        String originAiportID = removeDoubleQuotes(values[ORIGIN_AIRPORT_ID]);
        String destAirportID = removeDoubleQuotes(values[DEST_AIRPORT_ID]);
        String delayingTime = removeDoubleQuotes(values[ARR_DELAY]);
        String isCancelled = removeDoubleQuotes(values[CANCELLED]);


        if (delayingTime.isEmpty()) {
            return "";
        }

        float delay = Float.parseFloat(delayingTime);

        if (correctDelayingTime(delay)) {
            return "";
        }

        return new Tuple2<>(
                new Tuple2<>(destAirportID, delayingTime),
                new  FlightSerializable(),
        );
    }

    public void main(String[] args) throws Exception {
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

        JavaPairRDD<Tuple2<Integer, Integer>, String> flightRddPairs = flightRddRecords.mapToPair(
                x -> mapFlights(x)
        );
        JavaPairRDD<Integer, String> airportRddPairs = airportRddRecords.mapToPair();

        final Broadcast<Map<String, AirportData>> airportsBroadcasted = sctx.broadcast(stringAirportDataMap);
    }
}
