import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FileUtils;
import java.util.ArrayList;
import java.util.List;
import java.io.File;

public class MostPopularAircraftTypes {
    public static void main(String[] args) throws Exception {
        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // String localAirlineDataDir = "/Users/callumvandenhoek/Google Drive/Uni/DATA3404/Assignment/thing/ontimeperformance_airlines.csv";
        // String airlineDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_airlines.csv";
        // String localFlightDataDir = "/Users/callumvandenhoek/Google Drive/Uni/DATA3404/Assignment/thing/ontimeperformance_flights_tiny.csv";
        // String flightDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_flights_tiny.csv";
        // String localAircraftDataDir = "/Users/callumvandenhoek/Google Drive/Uni/DATA3404/Assignment/thing/ontimeperformance_aircrafts.csv";

        // default output file name
        String outputFileName = "result.txt";
        // load files from cluster
        String flightDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_tiny.csv";
        String airlineDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_airlines.csv";
        String aircraftDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_aircrafts.csv";

        // specify hadoop file from server
        if (args.length > 0) flightDataDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/data3404/assignment/ontimeperformance_flights_" + args[0] + ".csv";
        // specify output file name
        if (args.length > 1) outputFileName = args[1] + ".txt";

        // retrieve airline data from file: <airline_code, airline_name, country>
        DataSet<Tuple3<String, String, String>> airlines
            = env.readCsvFile(airlineDataDir)
              .includeFields("111")
              .ignoreFirstLine()
              .ignoreInvalidLines()
              .types(String.class, String.class, String.class);

        // retrieve flight data from file: <airline_code, tail_number>
        DataSet<Tuple2<String, String>> flights
            = env.readCsvFile(flightDataDir)
              .includeFields("010000100000")
              .ignoreFirstLine()
              .ignoreInvalidLines()
              .types(String.class, String.class);

        // retrieve aircraft data from file: <tailnum, manufacturer, model>
        DataSet<Tuple3<String, String, String>> aircraftDetails
            = env.readCsvFile(aircraftDataDir)
              .includeFields("101010000")
              .ignoreFirstLine()
              .ignoreInvalidLines()
              .types(String.class, String.class, String.class);

        // get all US airlines: <airline_code, airline_name>
        DataSet<Tuple2<String, String>> usAirline = airlines.reduceGroup(new USAirlineReducer());



        // join the result from "usAirline" and "flights"
        // to get airline names + tail numbers: <airline_name, tail_number>
        DataSet<Tuple2<String, String>> airlinesAndTailNumbers
            = usAirline.join(flights)
              .where(0) //carrier_code
              .equalTo(0) //carrier_code
              .with(new JoinALF()); // join airline and flight

        // join the result from "airlineTailNumbers" and "aircraftDetails"
        // to get airline name + tail numbers + aircraft details: <airline_name, tail_number, manufacturer, model>
        DataSet<Tuple4<String, String, String, String>> airlinesAndAircraftDetails
            = airlinesAndTailNumbers.join(aircraftDetails)
              .where(1) //tail_number in file
              .equalTo(0) //tailnum in file
              .with(new JoinALC()); //create new data set

        //Reduce dataset to create descending list of most used tailnumbers for each airline + count
        //<airline_name, tail_number, manufacturer, model, count>
        DataSet<Tuple5<String, String, String, String, Integer>> aircraftUsedCount
            = airlinesAndAircraftDetails.groupBy(1)// group the data by tailnumber
              .reduceGroup(new TailnumberCounter()) // for each group, count number of unique tailnumbers and output new data set including count
              .sortPartition(0, Order.ASCENDING).setParallelism(1) // sort by airline name
              .sortPartition(4, Order.DESCENDING); // sort by tailnumber count

        //Apply reduction so that only the 5 most used tailnumbers for each airline is recorded
        //Creates new data set of Tuples with fields: <airline_name, ArrayList <Tuple<manufacturer, model>>>
        DataSet<Tuple2<String, ArrayList<Tuple2<String, String>>>> aircraftUsedCountFive = aircraftUsedCount.reduceGroup(new FiveMostUsedReducer());

        // output file path
        String outPutDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/hche8927/output-t3/" + outputFileName;
        // use specified unikey
        if (args.length > 3) outPutDir = "hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/user/" + args[3] + "/output-t3/" + outputFileName;

        // store in hadoop cluster
        aircraftUsedCountFive.writeAsFormattedText(outPutDir, WriteMode.OVERWRITE,
        new TextFormatter<Tuple2<String, ArrayList<Tuple2<String, String>>>>() {
            public String format(Tuple2 <String, ArrayList<Tuple2<String, String>>> t) {
                String outputString = "";
                outputString += t.f0 + "\t[";
                for (int j = 0; j < t.f1.size(); j++) {
                    outputString += t.f1.get(j).f0 + " " + t.f1.get(j).f1;
                    if (!(j == (t.f1.size()) - 1)) outputString += ", ";
                }
                outputString += "]";
                
                return outputString;
            }
        });

        //Output results to file
        outputResults(aircraftUsedCountFive, outputFileName);

        // print results
        //aircraftUsedCount.print();
    }

    public static class TailnumberCounter implements GroupReduceFunction<Tuple4<String, String, String, String>, Tuple5<String, String, String, String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple4<String, String, String, String>> records, Collector<Tuple5<String, String, String, String, Integer>> out) throws Exception {
            String airline = null;
            String tailnumber = null;
            String manufacturer = null;
            String model = null;
            int cnt = 0;
            for (Tuple4<String, String, String, String> flight : records) {
                airline = flight.f0;
                tailnumber = flight.f1;
                manufacturer = flight.f2;
                model = flight.f3;
                cnt++;
            }
            out.collect(new Tuple5<String, String, String, String, Integer>(airline, tailnumber, manufacturer, model, cnt));
        }
    }

    private static class JoinALF implements JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> join(Tuple2<String, String> airline, Tuple2<String, String> flight) {
            return new Tuple2<String, String>(airline.f1, flight.f1);
        }
    }

    private static class JoinALC implements JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>> {
        @Override
        public Tuple4<String, String, String, String> join(Tuple2<String, String> airlineTailNumbers, Tuple3<String, String, String> aircraft) {
            return new Tuple4<String, String, String, String>(airlineTailNumbers.f0, airlineTailNumbers.f1, aircraft.f1, aircraft.f2);
        }
    }

    public static class USAirlineReducer implements GroupReduceFunction<Tuple3<String, String, String>, Tuple2<String, String>> {
        @Override
        public void reduce(Iterable<Tuple3<String, String, String>> records, Collector<Tuple2<String, String>> out) throws Exception {
            for (Tuple3<String, String, String> airline : records) {
                if (!"United States".equals(airline.f2)) continue;
                out.collect(new Tuple2<String, String>(airline.f0, airline.f1));
            }
        }
    }

    public static class FiveMostUsedReducer implements GroupReduceFunction<Tuple5<String, String, String, String, Integer>, Tuple2<String, ArrayList<Tuple2 <String, String>>>> {
        @Override
        public void reduce(Iterable<Tuple5<String, String, String, String, Integer>> records, Collector<Tuple2<String, ArrayList<Tuple2 <String, String>>>> out) throws Exception {
            String airlineName = "";
            ArrayList<String> modelMostUsed = new ArrayList<String>();
            ArrayList<Tuple2<String, String>> mostUsedList = new ArrayList<Tuple2<String, String>>();
            int counter = 0;        //Counter to limit output tuples to 5 per airline
            for (Tuple5<String, String, String, String, Integer> flight : records) {
                if (airlineName.equals("")) airlineName = flight.f0;
                if (counter == 5 || !(flight.f0.equals(airlineName))) {
                    if (flight.f0.equals(airlineName)) continue;
                    out.collect(new Tuple2<String, ArrayList<Tuple2<String, String>>>(airlineName, mostUsedList));
                    mostUsedList.clear();
                    modelMostUsed.clear();
                    counter = 0;
                    airlineName = flight.f0;
                }
                if (modelMostUsed.contains(flight.f3)) continue;
                counter++;
                modelMostUsed.add(flight.f3);
                mostUsedList.add(new Tuple2<String, String>(flight.f2, flight.f3));
            }
            out.collect(new Tuple2<String, ArrayList<Tuple2<String, String>>>(airlineName, mostUsedList));
        }
    }

    public static void outputResults(DataSet<Tuple2<String, ArrayList<Tuple2<String, String>>>> results, String outputFileName) throws Exception {
        List<Tuple2<String, ArrayList<Tuple2<String, String>>>> list = new ArrayList<Tuple2<String, ArrayList<Tuple2<String, String>>>>(results.collect());
        File outputFile = new File(outputFileName);
        outputFile.createNewFile();
        String outputString = "";

        for (int i = 0; i < list.size(); i++) {
            outputString += list.get(i).f0 + "\t[";
            for (int j = 0; j < list.get(i).f1.size(); j++) {
                outputString += list.get(i).f1.get(j).f0 + " " + list.get(i).f1.get(j).f1;
                if (!(j == (list.get(i).f1.size()) - 1)) outputString += ", ";
            }
            outputString += "]\n";
        }
        FileUtils.writeFileUtf8(outputFile, outputString);
    }

    public static void printResults(List<Tuple2<String, ArrayList<Tuple3<String, String, Integer>>>> results) throws Exception {

        System.out.println();
        for (int i = 0; i < results.size(); i++) {
            System.out.print(results.get(i).f0 + "\t[");
            for (int j = 0; j < results.get(i).f1.size(); j++) {
                System.out.print(results.get(i).f1.get(j).f0 + " " + results.get(i).f1.get(j).f1 + " (" + results.get(i).f1.get(j).f2 + ")");
                if (!(j == (results.get(i).f1.size()) - 1)) System.out.print(", ");
            }
            System.out.println("]");
        }
    }

}

