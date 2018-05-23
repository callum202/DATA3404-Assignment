import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;

public class TopThreeAirports {
    public static void main(String[] args) throws Exception {
        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String localAirportDatDir = "/media/sf_vm-shared-folder/data3404-workspace/assignment/assignment_data_files/ontimeperformance_airports.csv";
        String localFlightDataDir = "/media/sf_vm-shared-folder/data3404-workspace/assignment/assignment_data_files/ontimeperformance_flights_small.csv";
        String localSampleFlightDataDir = "/media/sf_vm-shared-folder/data3404-workspace/assignment/assignment_data_files/sample.csv";

        String airportDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_airports.csv";
        String flightDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_flights_small.csv";

        DataSet<Tuple3<String, String, String>> airports =
            env.readCsvFile(airportDataDir)
            .includeFields("1110000")
            .ignoreFirstLine()
            .ignoreInvalidLines()
            .types(String.class, String.class, String.class);

        DataSet<Tuple3<String, String, String>> flights =
            env.readCsvFile(localSampleFlightDataDir)
            .includeFields("100110000000")
            .ignoreFirstLine()
            .ignoreInvalidLines()
            .types(String.class, String.class, String.class);

        DataSet<Tuple5<String, String, String, String, String>> joinResult =
            airports.join(flights)
            .where(0)
            .equalTo(2)
            // .setParallelism(1)
            .with(new JoinAF());

        joinResult.print();
    }

    private static class JoinAF implements JoinFunction<Tuple3<String, String, String>,
                                                        Tuple3<String, String, String>,
                                                        Tuple5<String, String, String, String, String>> {
        @Override
        public Tuple5<String, String, String, String, String> join(
            Tuple3<String, String, String> first,
            Tuple3<String, String, String> second) {
            return new Tuple5<String, String, String, String, String>(second.f0, second.f1, first.f0, first.f1, first.f2);
        }
    }

}