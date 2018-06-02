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
import org.apache.flink.api.java.tuple.Tuple7;
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
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String localFlightDataDir = "/media/sf_vm-shared-folder/data3404-workspace/assignment_data_files/ontimeperformance_flights_medium.csv";
        String localAirlineDataDir = "/media/sf_vm-shared-folder/data3404-workspace/assignment_data_files/ontimeperformance_airlines.csv";

        String targetYear = "1994";
        String outputFileName = "result.txt";

        DataSet<Tuple3<String, String, String>> airline
            = env.readCsvFile(localAirlineDataDir)
              .includeFields("111")
              .ignoreFirstLine()
              .ignoreInvalidLines()
              .types(String.class, String.class, String.class);

        DataSet<Tuple6<String, String, String, String, String, String>> flights
            = env.readCsvFile(localFlightDataDir)
              .includeFields("010100011110")
              .ignoreFirstLine()
              .ignoreInvalidLines()
              .types(String.class, String.class, String.class, String.class, String.class, String.class);

        DataSet<Tuple2<String, String>> usAirline = airline.reduceGroup(new USAirlineReducer());

        DataSet<Tuple7<String, String, String, String, String, String, String>> airlineFlights
            = usAirline.join(flights)
              .where(0)
              .equalTo(0)
              .with(new JoinALF());

        DataSet<Tuple2<String, Double>> airlineDelays = airlineFlights.reduceGroup(new YearDelayReducer(targetYear));

        // the result
        DataSet<Tuple2<String, Double>> result
            = airlineDelays.groupBy(0)
              .reduceGroup(new avgDelay())
              .sortPartition(0, Order.ASCENDING).setParallelism(1);

        result.writeAsCsv("temp.csv", WriteMode.OVERWRITE);
        env.execute();
    }

    private static class JoinALF implements JoinFunction<Tuple2<String, String>, Tuple6<String, String, String, String, String, String>, Tuple7<String, String, String, String, String, String, String>> {
        @Override
        public Tuple7<String, String, String, String, String, String, String> join(Tuple2<String, String> airline, Tuple6<String, String, String, String, String, String> flight) {
            return new Tuple7<String, String, String, String, String, String, String>(airline.f0, airline.f1, flight.f1, flight.f2, flight.f3, flight.f4, flight.f5);
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

    public static class YearDelayReducer implements GroupReduceFunction<Tuple7<String, String, String, String, String, String, String>, Tuple2<String, Double>> {
        // target year
        private String year;

        // constructor
        YearDelayReducer(String year) {
            this.year = year;
        }

        @Override
        public void reduce(Iterable<Tuple7<String, String, String, String, String, String, String>> records, Collector<Tuple2<String, Double>> out) throws Exception {
            for (Tuple7<String, String, String, String, String, String, String> flight : records) {
                if (!flight.f2.contains(year)) continue;
                if (flight.f3.equals("") || flight.f4.equals("") || flight.f5.equals("") || flight.f6.equals("") ) continue;

                DateFormat formatter = new SimpleDateFormat("HH:mm:ss");

                Date expectDepart = (Date)formatter.parse(flight.f3);
                Date actualDepart = (Date)formatter.parse(flight.f5);
                Date expectArrive = (Date)formatter.parse(flight.f4);
                Date actualArrive = (Date)formatter.parse(flight.f6);

                long departDelay = actualDepart.getTime() - expectDepart.getTime();
                departDelay = departDelay > 0 ? departDelay : 0;
                long arriveDelay = actualArrive.getTime() - expectArrive.getTime();
                arriveDelay = arriveDelay > 0 ? arriveDelay : 0;

                Double delay = (double) (departDelay + arriveDelay);
                out.collect(new Tuple2<String, Double>(flight.f1, delay));
            }
        }
    }

    public static class avgDelay implements GroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
        @Override
        public void reduce(Iterable<Tuple2<String, Double>> records, Collector<Tuple2<String, Double>> out) throws Exception {
            String airport = null;

            double totalDelay = 0;
            int cnt = 0;

            for (Tuple2<String, Double> r : records) {
                airport = r.f0;
                totalDelay += r.f1;
                cnt++;
            }

            out.collect(new Tuple2<String, Double>(airport, totalDelay / cnt / 60000.0));
        }
    }
}