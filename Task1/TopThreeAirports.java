import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
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
        // default year
        String targetYear = "1994";

        // specify which year we want to retrieve
        if (args.length == 1) {
            targetYear = args[0];
        }

        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // use the local file, CHANGE this if needed
        String localFlightDataDir = "/media/sf_vm-shared-folder/data3404-workspace/DATA3404-Assignment/assignment_data_files/ontimeperformance_flights_tiny.csv";
        // use the file stored in hadoop, CHANGE this if needed
        String flightDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_flights_tiny.csv";

        // retrieve data from file
        DataSet<Tuple2<String, String>> flights = env.readCsvFile(flightDataDir)
                                                    .includeFields("000110000000") // (date, airport_code)
                                                    .ignoreFirstLine()
                                                    .ignoreInvalidLines()
                                                    .types(String.class, String.class); // specify type for each field

        // filter out undesired tuples
        DataSet<Tuple2<String, String>> yearReduceResult = flights.reduceGroup(new YearReducer(targetYear));

        // the result
        DataSet<Tuple2<String, Integer>> countResult = yearReduceResult.groupBy(1) // group the data by airport code
                                                                    .reduceGroup(new AirportCounter()) // for each group, apply the "GroupReduceFunction"
                                                                    .sortPartition(1, Order.DESCENDING).setParallelism(1); // sort by number of flights from reduction result

        File outputFile = new File("out.txt");
        outputFile.createNewFile();
        String outputString = "";
        List<Tuple2<String, Integer>> resultTuples = countResult.collect();
        for (int i = 0; i < 3; i++) {
            outputString += resultTuples.get(i).getField(0) + "\t" + resultTuples.get(i).getField(1) + "\n";
        }
        FileUtils.writeFileUtf8(outputFile, outputString);

        // get top 3 results and print them
        countResult.first(3).print();
    }

    // NOTE: GroupReduceFunction<type of input, type of output>
    // in this case GroupReduceFunction<Tuple2<DATE, AIRLINE_CODE>, Tuple2<DATE, AIRLINE_CODE>>
    //
    // Only return flight record from give year
    public static class YearReducer implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>> {
        // target year
        private String year;

        // constructor
        YearReducer(String year) {
            this.year = year;
        }

        @Override
        public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple2<String, String>> out) throws Exception {
            for (Tuple2<String, String> flight : records) {
                if (!flight.f0.contains(year)) continue;
                out.collect(flight);
            }
        }
    }

    // NOTE: In this case GroupReduceFunction<Tuple2<DATE, AIRLINE_CODE>, Tuple2<AIRLINE_CODE, COUNTER>>
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

}
