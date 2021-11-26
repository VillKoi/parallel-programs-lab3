package flights;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class FlightApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: AirportsApp <1: input path FlightMapper> <2: input path AirportMapper> <output path>");
            System.exit(-1);
        }

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
    }
}
