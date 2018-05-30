import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.File;

public class TopThreeAirports {
    public static void main(String[] args) throws Exception {
        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // default year
        String targetYear = "1994";
        // default output file name
        String outputFileName = "result.txt";
        // use the local file
        String localFlightDataDir = "/media/sf_vm-shared-folder/data3404-workspace/DATA3404-Assignment/assignment_data_files/ontimeperformance_flights_tiny.csv";
        // use local hadoop file
        String flightDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_flights_tiny.csv";
        
        // specify year
        if (args.length > 0) targetYear = args[0];
        // specify hadoop file from server
        if (args.length > 1) flightDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_" + args[1] + ".csv";
        // specify output file name
        if (args.length > 2) outputFileName = args[2] + ".txt";

        // retrieve data from file
        DataSet<Tuple3<String, String, String>> flights = env.readCsvFile(flightDataDir)
                                                        .includeFields("000110000100") // (date, airport_code)
                                                        .ignoreFirstLine()
                                                        .ignoreInvalidLines()
                                                        .types(String.class, String.class, String.class); // specify type for each field

        // filter out undesired tuples
        DataSet<Tuple2<String, String>> yearReduceResult = flights.reduceGroup(new YearReducer(targetYear));

        // the result
        DataSet<Tuple2<String, Integer>> topThreeResult = yearReduceResult.groupBy(1) // group the data by airport code
                                                                        .reduceGroup(new AirportCounter()) // for each group, apply the "GroupReduceFunction"
                                                                        .sortPartition(1, Order.DESCENDING) // sort by number of flights from reduction result
                                                                        .setParallelism(1) // prevent incorrect order
                                                                        .first(3); // get top 3 results

        // store in hadoop
        topThreeResult.writeAsFormattedText("hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/hche8927/output/" + outputFileName, WriteMode.OVERWRITE,
            new TextFormatter<Tuple2<String, Integer>>() {
                public String format(Tuple2<String, Integer> t) {
                    return t.f0 + "\t" + t.f1;
                }
            }
        );

        // print results
        topThreeResult.print();
        // store locally
        saveLocalResults(topThreeResult, outputFileName);
    }

    // GroupReduceFunction<Tuple2<DATE, AIRLINE_CODE, DEPART_TIME>, Tuple2<DATE, AIRLINE_CODE>>
    //
    // Only return flight record from give year
    public static class YearReducer implements GroupReduceFunction<Tuple3<String, String, String>, Tuple2<String, String>> {
        // target year
        private String year;

        // constructor
        YearReducer(String year) {
            this.year = year;
        }

        @Override
        public void reduce(Iterable<Tuple3<String, String, String>> records, Collector<Tuple2<String, String>> out) throws Exception {
            for (Tuple3<String, String, String> flight : records) {
                if (!flight.f0.contains(year)) continue;
                if (flight.f2.equals("")) continue;
                out.collect(new Tuple2<String, String>(flight.f0, flight.f1));
            }
        }
    }

    // GroupReduceFunction<Tuple2<DATE, AIRLINE_CODE>, Tuple2<AIRLINE_CODE, COUNTER>>
    //
    // Count number of records for each group (grouped by airline_code)
    public static class AirportCounter implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple2<String, Integer>> out) throws Exception {
            String airport = null;
            int cnt = 0;
            for (Tuple2<String, String> r : records) {
                airport = r.f1;
                cnt++;
            }
            out.collect(new Tuple2<>(airport, cnt));
        }
    }

    // Utility funtion for saving local copy of the result
    public static void saveLocalResults(DataSet<Tuple2<String, Integer>> results, String outputFileName) throws Exception {
        File outputFile = new File(outputFileName);
        outputFile.createNewFile();
        String outputString = "";
        List<Tuple2<String, Integer>> resultTuples = results.collect();
        for (Tuple2<String, Integer> t : resultTuples) {
            outputString += t.getField(0) + "\t" + t.getField(1) + "\n";
        }
        FileUtils.writeFileUtf8(outputFile, outputString);
    }
}
