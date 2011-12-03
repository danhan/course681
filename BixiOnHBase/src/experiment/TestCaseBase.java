package experiment;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import bixi.hbase.query.BixiQueryAbstraction;

public abstract class TestCaseBase {
	
	private static final double RADIUS = 100;
	
	BixiQueryAbstraction bixiQuery;
	Properties tests;
	int times = 3;
	
	abstract BixiQueryAbstraction getBixiQuery();
	protected String convertDate(String a){
		return a;
	}
	
	private static String[] shortTestByTime  = {};
	private static String[] shortTestByStation  = {};
	private static String[] longTestByTime  = {};
	private static String[] longTestByStation  = {};
	
	
	protected static final String[] pointTests = {"location"};
	protected static final String singleTimeStamp = "singleTimestamp";

	public TestCaseBase() {		
		
		tests = new Properties();
		try {
		    tests.load(new FileInputStream("tests.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}				
		shortTestByTime = tests.getProperty("shortTestByTime").trim().split(";");		
		shortTestByStation = tests.getProperty("shortTestByStation").trim().split(";");
		longTestByTime = tests.getProperty("longTestByTime").trim().split(";");
		longTestByStation = tests.getProperty("longTestByStation").trim().split(";");
		
		bixiQuery = getBixiQuery();
		
		
	}
	
	public void runTests(){
//		System.out.println("********Short Analysis**by time******callTimeSlot4StationsScan**************");
//		for(String test : shortTestByTime){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4StationsScan(test);
//			System.out.println();
//		}
//
//		System.out.println("**********Long Analysis*by time*****callTimeSlot4StationsScan******************");
//		for(String test : longTestByTime){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4StationsScan(test);
//			System.out.println();
//		}

//		System.out.println("********Short Analysis by station***callTimeSlot4StationsScan**************");
//		for(String test : shortTestByStation){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4StationsScan(test);
//			System.out.println();
//		}
		
//		System.out.println("**********Long Analysis by station******callTimeSlot4StationsScan******************");
//		for(String test : longTestByStation){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4StationsScan(test);
//			System.out.println();
//		}		
//		
//		
//		System.out.println("****************callTimeStamp4PointScan*******************************");
//		for(String test : pointTests){
//			System.out.println("====Start========="+test+"==============");
//			callTimeStamp4PointScan(test);
//			System.out.println();		
//		}		
		
		System.out.println("=============================Coprocessor===============================");
		System.out.println("***********Long Analysis*by time****callTimeSlot4Stations**********************");
		for(String test : longTestByTime){
			System.out.println("====Start========="+test+"==============");
			callTimeSlot4Stations(test);
			System.out.println();
		}	
		

		
		System.out.println("***********Long Analysis by station******callTimeSlot4Stations**********************");
		for(String test : longTestByStation){
			System.out.println("====Start========="+test+"==============");
			callTimeSlot4Stations(test);
			System.out.println();
		}		
		
		System.out.println("******************callTimeStamp4Point**********************************");
		for(String test : pointTests){
			System.out.println("====Start========="+test+"==============");
			callTimeStamp4Point(test);
			System.out.println();
		}
		

		
	}
	
	/* Private Methods */
	
	private void callTimeSlot4Stations(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");
		String start = convertDate(args[0]);
		String end = convertDate(args[1]);
		String stations = args[2];
		for(int i=0;i<times;i++){
			System.out.println("#####################"+i+" Time #########################");
			bixiQuery.queryAvgUsageByTimeSlot4Stations(start, end, stations);
			System.out.println("########################################################\n");
		}
			
	}
	private void callTimeSlot4StationsScan(String propertyName){
		String property = tests.getProperty(propertyName);		
		String[] args = property.split(" ");
		String start = convertDate(args[0]);
		String end = convertDate(args[1]);
		String stations = args[2];
		System.out.println(start + "; "+end + ";" + stations);
		bixiQuery.queryAvgUsageByTimeSlot4StationsWithScan(start, end, stations);
	}
	private void callTimeStamp4Point(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");
		String timestamp = convertDate(args[0]);
		Double latitude = Double.parseDouble(args[1]);
		Double longitude = Double.parseDouble(args[2]);
		bixiQuery.queryAvailableByTimeStamp4Point(timestamp, latitude, longitude, RADIUS);
	}
	private void callTimeStamp4PointScan(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");
		String timestamp = convertDate(args[0]);
		Double latitude = Double.parseDouble(args[1]);
		Double longitude = Double.parseDouble(args[2]);
		bixiQuery.queryAvailableByTimeStamp4PointWithScan(timestamp, latitude, longitude, RADIUS);
	}
	
}


/*		
System.out.println("*******Short Analysis***by time********callTimeSlot4Stations******************");
for(String test : shortTestByTime){
	System.out.println("====Start========="+test+"==============");
	callTimeSlot4Stations(test);
	System.out.println("====End========="+test+"==============");
}	

System.out.println("*******Short Analysis*** by station**callTimeSlot4Stations******************");
for(String test : shortTestByStation){
	System.out.println("====Start========="+test+"==============");
	callTimeSlot4Stations(test);
	System.out.println("====End========="+test+"==============");
}				
*/	