import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.io.File;

public class AverageFlightDelay {
    public static void main(String[] args) throws Exception {
        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String localFlightDataDir = "/media/sf_vm-shared-folder/data3404-workspace/assignment_data_files/ontimeperformance_flights_medium.csv";
        // String flightDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_flights_tiny.csv";
        String localAirlineDataDir = "/media/sf_vm-shared-folder/data3404-workspace/assignment_data_files/ontimeperformance_airlines.csv";
        // String airlineDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_airlines.csv";

        // default year
        String targetYear = "1994";
        // default output file name
        String outputFileName = "result.txt";
        // load files from cluster
        String flightDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_tiny.csv";
        String airlineDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_airlines.csv";

        // specify year
        if (args.length > 0) targetYear = args[0];
        // specify hadoop file from server
        if (args.length > 1) flightDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_" + args[1] + ".csv";
        // specify output file name
        if (args.length > 2) outputFileName = args[2] + ".txt";

        // retrieve flight data from file: <airline_code, airline_name, airline_country>
        DataSet<Tuple3<String, String, String>> airline
            = env.readCsvFile(localAirlineDataDir)
              .includeFields("111")
              .ignoreFirstLine()
              .ignoreInvalidLines()
              .types(String.class, String.class, String.class);

        // retrieve airpots data from file: <airline_code, flight_date, expect_depart, actual_depart>
        DataSet<Tuple6<String, String, String, String, String, String>> flights
            = env.readCsvFile(localFlightDataDir)
              .includeFields("010100011110")
              .ignoreFirstLine()
              .ignoreInvalidLines()
              .types(String.class, String.class, String.class, String.class, String.class, String.class);

        // get all US airlines: <airline_code, airline_name>
        DataSet<Tuple2<String, String>> usAirline = airline.reduceGroup(new USAirlineReducer());

        // get all flight delays: <airline_code, delay>
        DataSet<Tuple2<String, Double>> flightDelays = flights.reduceGroup(new YearDelayReducer(targetYear));

        // join the result from "usAirline" and "flightDelays"
        // to get airline flight delays: <airline_code, airline_name, delay>
        DataSet<Tuple3<String, String, Double>> airlineFlightDelays = usAirline.join(flightDelays)
                .where(0)
                .equalTo(0)
                .with(new JoinALF()); // join airline and flight

        // the result
        DataSet<Tuple2<String, Double>> result = airlineFlightDelays.groupBy(0)
                .reduceGroup(new avgDelay())
                .sortPartition(0, Order.ASCENDING).setParallelism(1);

        // output file path
        String outPutDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/hche8927/output-t2/" + outputFileName;
        // use specified unikey
        if (args.length > 3) outPutDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/" + args[3] + "/output-t2/" + outputFileName;

        // store in hadoop cluster
        // result.writeAsFormattedText(outPutDir, WriteMode.OVERWRITE,
        // new TextFormatter<Tuple2<String, Double>>() {
        //     public String format(Tuple2<String, Double> t) {
        //         return t.f0 + "\t" + t.f1;
        //     }
        // });

        // save to local
        outputResults(result, outputFileName);

        // get top 3 results and print them
        // result.print();
    }

    private static class JoinALF implements JoinFunction<Tuple2<String, String>, Tuple2<String, Double>, Tuple3<String, String, Double>> {
        @Override
        public Tuple3<String, String, Double> join(Tuple2<String, String> airline, Tuple2<String, Double> flight) {
            return new Tuple3<String, String, Double>(airline.f0, airline.f1, flight.f1);
        }
    }

    public static class USAirlineReducer implements GroupReduceFunction<Tuple3<String, String, String>, Tuple2<String, String>> {
        @Override
        public void reduce(Iterable<Tuple3<String, String, String>> records, Collector<Tuple2<String, String>> out) throws Exception {
            for (Tuple3<String, String, String> airline : records) {
                if (!airline.f2.equals("United States")) continue;
                out.collect(new Tuple2<String, String>(airline.f0, airline.f1));
            }
        }
    }

    public static class YearDelayReducer implements GroupReduceFunction<Tuple6<String, String, String, String, String, String>, Tuple2<String, Double>> {
        // target year
        private String year;

        // constructor
        YearDelayReducer(String year) {
            this.year = year;
        }

        @Override
        public void reduce(Iterable<Tuple6<String, String, String, String, String, String>> records, Collector<Tuple2<String, Double>> out) throws Exception {
            for (Tuple6<String, String, String, String, String, String> flight : records) {
                if (!flight.f1.contains(year)) continue;
                if (flight.f2.equals("") || flight.f3.equals("") || flight.f4.equals("") || flight.f5.equals("") ) continue;

                DateFormat formatter = new SimpleDateFormat("HH:mm:ss");

                Date expectDepart = (Date)formatter.parse(flight.f2);
                Date actualDepart = (Date)formatter.parse(flight.f4);
                Date expectArrive = (Date)formatter.parse(flight.f3);
                Date actualArrive = (Date)formatter.parse(flight.f5);

                long departDelay = actualDepart.getTime() - expectDepart.getTime();
                departDelay = departDelay > 0 ? departDelay : 0;
                long arriveDelay = actualArrive.getTime() - expectArrive.getTime();
                arriveDelay = arriveDelay > 0 ? arriveDelay : 0;

                Double delay = (double) (departDelay + arriveDelay);
                out.collect(new Tuple2<String, Double>(flight.f0, delay));
            }
        }
    }

    public static class avgDelay implements GroupReduceFunction<Tuple3<String, String, Double>, Tuple2<String, Double>> {
        @Override
        public void reduce(Iterable<Tuple3<String, String, Double>> records, Collector<Tuple2<String, Double>> out) throws Exception {
            String airport = null;

            double totalDelay = 0;
            int cnt = 0;

            for (Tuple3<String, String, Double> r : records) {
                airport = r.f1;
                totalDelay += r.f2;
                cnt++;
            }

            out.collect(new Tuple2<String, Double>(airport, totalDelay / cnt / 60000.0));
        }
    }

    public static void outputResults(DataSet<Tuple2<String, Double>> result, String outputFileName) throws Exception {
        File outputFile = new File(outputFileName);
        outputFile.createNewFile();
        String outputString = "";
        List<Tuple2<String, Double>> resultTuples = result.collect();
        for (Tuple2<String, Double> t : resultTuples) {
            outputString += t.getField(0) + "\t" + t.getField(1) + "\n";
        }
        FileUtils.writeFileUtf8(outputFile, outputString);
    }

}
