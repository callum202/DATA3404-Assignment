import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;



import org.apache.flink.api.common.operators.Order;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class MostPopularAircraftTypes {
    public static void main(String[] args) throws Exception {
        // default year
        String targetYear = "1994";

        // specify which year we want to retrieve
        if (args.length == 1) {
            targetYear = args[0];
        }

        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String localAirlineDataDir = "/Users/callumvandenhoek/Google Drive/Uni/DATA3404/Assignment/thing/ontimeperformance_airlines.csv";
        String airlineDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_airlines.csv";

        String localFlightDataDir = "/Users/callumvandenhoek/Google Drive/Uni/DATA3404/Assignment/thing/ontimeperformance_flights_tiny.csv";
        String flightDataDir = "hdfs://localhost:9000/user/hche8927/assignment-data/ontimeperformance_flights_tiny.csv";
        
        String localAircraftDataDir = "/Users/callumvandenhoek/Google Drive/Uni/DATA3404/Assignment/thing/ontimeperformance_aircrafts.csv";

        // retrieve airline data from file: <airline_code, airline_name, country>
        DataSet<Tuple3<String, String, String>> airline = env.readCsvFile(localAirlineDataDir)
                                                        .includeFields("111")
                                                        .ignoreFirstLine()
                                                        .ignoreInvalidLines()
                                                        .types(String.class,String.class, String.class);

        // retrieve flight data from file: <airline_code, tail_number>
        DataSet<Tuple2<String, String>> flights = env.readCsvFile(localFlightDataDir)
                                                            .includeFields("010000100000")
                                                            .ignoreFirstLine()
                                                            .ignoreInvalidLines()
                                                            .types(String.class, String.class);
        
        // retrieve aircraft data from file: <tailnum, manufacturer, model>
        DataSet<Tuple3<String, String, String>> aircraft = env.readCsvFile(localAircraftDataDir)
                                                            .includeFields("101010000")
                                                            .ignoreFirstLine()
                                                            .ignoreInvalidLines()
                                                            .types(String.class, String.class, String.class);
        
        // get all US airlines: <airline_code, airline_name>
        DataSet<Tuple2<String, String>> usAirline = airline.reduceGroup(new USAirlineReducer());
        													  


        // join the result from "usAirline" and "flights" 
        // to get airline tail numbers: <airline_name, tail_number>
        DataSet<Tuple2<String, String>> airlineTailNumbers = usAirline.join(flights)
                                                                                .where(0)
                                                                                .equalTo(0)
                                                                                .with(new JoinALF()); // join airline and flight
        
        // join the result from "airlineTailNumbers" and "aircraftDetails" 
        // to get airline tail numbers + aircraft details: <airline_name, tail_number, manufacturer, model> --- Not used yet!
        DataSet<Tuple4<String, String, String, String>> aircraftDetails = airlineTailNumbers.join(aircraft)
																               .where(1)
																               .equalTo(0)
																               .with(new JoinALC()); // join airline and aircraft
        
        //Reduce dataset to create descending list of most used tailnumbers for each airline + count
        DataSet<Tuple5<String, String, String, String, Integer>> countResult = aircraftDetails.groupBy(1)// group the data by tailnumber 
												                .reduceGroup(new AircraftCounter()) // for each group, apply the "GroupReduceFunction"
												                	.sortPartition(0, Order.ASCENDING).setParallelism(1) // sort by airline (not working as it should be - creates several separately alphabetized groups instead of one big alphabetized groups)
												                .sortPartition(2, Order.DESCENDING); // sort from most used tailnumber						
        	
        //Apply reduction so that only the 5 most used tailnumbers for each airline is recorded
        DataSet<Tuple2<String, ArrayList<Tuple2<String, String>>>> reduceResult = countResult.reduceGroup(new AirlineReducer());
        
        //Print results
        List<Tuple2<String,ArrayList<Tuple2<String,String>>>> reduceResultList = new ArrayList<Tuple2<String,ArrayList<Tuple2<String,String>>>>(reduceResult.collect());
        printResults(reduceResultList);
        
        

    }
    
    public static class AircraftCounter implements GroupReduceFunction<Tuple4<String, String, String, String>, Tuple5<String, String, String, String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple4<String, String, String, String>> records, Collector<Tuple5<String, String, String, String, Integer>> out) throws Exception {
        		String airline = null;
            String tailnumber = null;
            String manufacturer = null;
            String model = null;
            int cnt = 0;
            for (Tuple4<String, String, String, String> r : records) {
            		airline = r.f0;
            		tailnumber = r.f1;
            		manufacturer = r.f2;
            		model = r.f3;
                if (tailnumber.matches("N[a-zA-Z0-9]*")||tailnumber.matches("[a-zA-Z0-9]*9E")) cnt++;	
                //Get rid of empty tailnumbers or tailnumbers which don't follow format of N____ or ____9E (not sure about whether ____9E should be included as it has no aircraft manufacturer/model info linked)
            }
            out.collect(new Tuple5<String, String, String, String, Integer>(airline,tailnumber,manufacturer, model, cnt));
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
        public Tuple4<String, String, String, String> join(Tuple2<String, String> airlineTailNumbers, Tuple3<String,String, String> aircraft) {
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
    
    public static class AirlineReducer implements GroupReduceFunction<Tuple5<String, String, String, String, Integer>, Tuple2<String, ArrayList<Tuple2 <String, String>>>> {
        @Override
        public void reduce(Iterable<Tuple5<String, String, String, String, Integer>> records, Collector<Tuple2<String, ArrayList<Tuple2 <String, String>>>> out) throws Exception {
        		String airlineName = "";
        		ArrayList<Tuple2<String, String>> mostUsedList = new ArrayList<Tuple2<String, String>>();
        		int counter = 0;		//Counter to limit output tuples to 5 per airline
            for (Tuple5<String, String, String, String, Integer> airline : records) {
            		if (airlineName.equals("")) airlineName=airline.f0;
            		if (counter==5) {
            			if (airline.f0.equals(airlineName)) continue;
            			else counter = 0;
            			out.collect(new Tuple2<String, ArrayList<Tuple2<String, String>>>(airlineName,mostUsedList));
            			mostUsedList.clear();
            		}
            		if (!airlineName.equals(airline.f0)) {
            			counter=0;
            			airlineName=airline.f0;
            		}
            		counter++;
            		mostUsedList.add(new Tuple2<String, String>(airline.f2,airline.f3));
            }
        }
    }
    
	public static void printResults(List<Tuple2<String, ArrayList<Tuple2<String, String>>>> results) throws Exception{
		
		System.out.println();
		for (int i=0;i<results.size();i++) {
			System.out.print(results.get(i).f0+"\t[");
			for (int j=0;j<results.get(i).f1.size();j++) {
				System.out.print(results.get(i).f1.get(j).f0+" "+results.get(i).f1.get(j).f1);
				if (!(j==(results.get(i).f1.size())-1)) System.out.print(", ");
			}
			System.out.println("]");
		}
	}
    	
}
    