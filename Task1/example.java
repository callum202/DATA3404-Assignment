import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;
import java.util.List;

public class TopThreeAirports {
    public static void main(String[] args) throws Exception {
        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String localAirportDatDir = "/media/sf_vm-shared-folder/data3404-workspace/DATA3404-Assignment/assignment_data_files/ontimeperformance_airports.csv";
        String localFlightDataDir = "/media/sf_vm-shared-folder/data3404-workspace/DATA3404-Assignment/assignment_data_files/ontimeperformance_flights_small.csv";

        String airportDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_airports.csv";
        String flightDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_flights_tiny.csv";

        DataSet<Tuple3<String, String, String>> airports = env.readCsvFile(airportDataDir)
                                                                .includeFields("1110000")
                                                                .ignoreFirstLine()
                                                                .ignoreInvalidLines()
                                                                .types(String.class, String.class, String.class);

        DataSet<Tuple3<String, String, String>> flights = env.readCsvFile(flightDataDir)
                                                                .includeFields("100110000000")
                                                                .ignoreFirstLine()
                                                                .ignoreInvalidLines()
                                                                .types(String.class, String.class, String.class);

        DataSet<Tuple5<String, String, String, String, String>> joinResult = airports.join(flights)
                                                                                        .where(0)
                                                                                        .equalTo(2)
                                                                                        .with(new JoinAF());

        DataSet<Tuple5<String, String, String, String, String>> reduceByYearResult = joinResult.groupBy();

        DataSet<Tuple2<String, Integer>> countResult = joinResult.groupBy(2)
                                                                    .reduceGroup(new ACounter())
                                                                    .sortPartition(1, Order.DESCENDING);
        
        countResult.first(3).print();

        // List<Tuple2<String, Integer>> rankedList = countResult.collect();
        // for (int i = 0; i < 3; i++) {
        //     System.out.println(rankedList.get(i));
        // }

    }

    private static class JoinAF implements JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple5<String, String, String, String, String>> {
        @Override
        public Tuple5<String, String, String, String, String> join(Tuple3<String, String, String> first, Tuple3<String, String, String> second) {
            return new Tuple5<String, String, String, String, String>(second.f0, second.f1, first.f0, first.f1, first.f2);
        }
    }

    public static class ACounter implements GroupReduceFunction<Tuple5<String, String, String, String, String>, Tuple2<String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple5<String, String, String, String, String>> records, Collector<Tuple2<String, Integer>> out) throws Exception {
            String airport = null;
            int cnt = 0;
            for (Tuple5<String, String, String, String, String> a : records) {
                airport = a.f2;
                cnt++;
            }
            out.collect(new Tuple2<>(airport, cnt));
        }
    }

}