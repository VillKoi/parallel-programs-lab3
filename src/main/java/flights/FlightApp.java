package flights;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Iterator;

public class FlightApp {
    private static final Function2<Integer, Iterator<String>, Iterator<String>> handlingCVS  = new Function2<Integer, Iterator<String>, Iterator<String>>() {
        public Iterator<String> call(Integer index, Iterator<String> iter) {
            if (index == 0 && iter.hasNext()) {
                iter.next();
                return iter;
            }
            return iter;
        }
    };

    private static final String FLIGHT_STRING_SPLITTER = ",";
    private static final String DOUBLE_QUOTES = "\"";
    // number in cvs
    private static final int DEST_AIRPORT_ID = 14;
    private static final int ORIGIN_AIRPORT_ID = 11;
    private static final int ARR_DELAY = 18;
    private static final int CANCELLED = 20;

    private static String removeDoubleQuotes(String value) {
        return value.replaceAll(DOUBLE_QUOTES, "");
    }

    private static Tuple2<Tuple2<Integer, Integer>, FlightSerializable>  mapFlights(String text) {
        String[] values = text.split(FLIGHT_STRING_SPLITTER);

        Integer originAirportID = Integer.parseInt(removeDoubleQuotes(values[ORIGIN_AIRPORT_ID]));
        Integer destAirportID = Integer.parseInt(removeDoubleQuotes(values[DEST_AIRPORT_ID]));
        String delayingTime = removeDoubleQuotes(values[ARR_DELAY]);
        boolean isCancelled = !removeDoubleQuotes(values[CANCELLED]).isEmpty();

        float delay = delayingTime.isEmpty() ? 0 : Float.parseFloat(delayingTime);

        return new Tuple2<>(
                new Tuple2<>(originAirportID, destAirportID),
                new  FlightSerializable(delay, isCancelled)
        );
    }

    private static final String AIRPORT_STRING_SPLITTER = ",";
    private static final int AIRPORT_ID_NUMBER = 0;
    private static final int AIRPORT_NAME_NUMBER = 1;

    private static Tuple2<Integer, AirportSerializable>  mapAirports(String text) {
        String[] values = text.split(AIRPORT_STRING_SPLITTER);

        Integer airportID =  Integer.parseInt( removeDoubleQuotes(values[AIRPORT_ID_NUMBER]));
        String airportName = removeDoubleQuotes(values[AIRPORT_NAME_NUMBER]);

        return new Tuple2<>(airportID, new AirportSerializable(airportID, airportName));
    }

    private static Tuple2<Tuple2<Integer, Integer>, FlightSerializable>  reduceFlights(FlightSerializable flight) {

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

        JavaPairRDD<Tuple2<Integer, Integer>, FlightSerializable> flightRddPairs = flightRddRecords
                .mapToPair(x -> mapFlights(x))
                .reduce(x - > reduceFlights(x));

        JavaPairRDD<Integer, AirportSerializable> airportRddPairs = airportRddRecords
                .mapToPair(x -> mapAirports(x)
        );

        final Broadcast<Map<String, AirportData>> airportsBroadcasted = sctx.broadcast(airportRddRecords);
    }
}
