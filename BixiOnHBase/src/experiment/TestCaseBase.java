package experiment;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.QueryAbstraction;
import bixi.hbase.query.location.BixiLocationQueryS1;

public abstract class TestCaseBase {
	
	private static final double RADIUS = 100;
	
	BixiQueryAbstraction bixiQuery;
	Properties tests;
	int times = 1;
	
	abstract BixiQueryAbstraction getBixiQuery();
	QueryAbstraction getBixiLocationQuery(){
		return null;
	}
	protected String convertDate(String a){
		return a;
	}
	
	private static String[] s1_by_time  = {};
	private static String[] s10_by_time  = {};
	private static String[] s100_by_time  = {};
	private static String[] s200_by_time  = {};
	
	
	protected static String[] locations = {};
	protected static final String singleTimeStamp = "singleTimestamp";

	public TestCaseBase() {		
		
		tests = new Properties();
		try {
			if(!new File("tests.properties").exists()){
				tests.load(new FileInputStream("./command/tests.properties"));
			}else{
				 tests.load(new FileInputStream("tests.properties"));
			}
		   
		} catch (IOException e) {
			e.printStackTrace();
		}				
		s1_by_time = tests.getProperty("s1_by_time").trim().split(";");		
		s10_by_time = tests.getProperty("s10_by_time").trim().split(";");
		s100_by_time = tests.getProperty("s100_by_time").trim().split(";");
		s200_by_time = tests.getProperty("s200_by_time").trim().split(";");
		locations = tests.getProperty("locations").trim().split(";");
		
		if(tests.getProperty("run_times") != null)
			times = Integer.valueOf(tests.getProperty("run_times"));
		
		bixiQuery = getBixiQuery();
				
	}
	
	public void runTests(){		
//		System.out.println("********Short Analysis**by time******callTimeSlot4StationsScan**************");
//		for(String test : s1_by_time){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4StationsScan(test);
//			System.out.println();
//		}
//		System.out.println("********Short Analysis by station***callTimeSlot4StationsScan**************");
//		for(String test : s10_by_time){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4StationsScan(test);
//			System.out.println();
//		}
		
//		System.out.println("**********Long Analysis*by time*****callTimeSlot4StationsScan******************");
//		for(String test : s100_by_time){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4StationsScan(test);
//			System.out.println();
//		}
//

//		
//		System.out.println("**********Long Analysis by station******callTimeSlot4StationsScan******************");
//		for(String test : s200_by_time){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4StationsScan(test);
//			System.out.println();
//		}		
//		
//				
//		
//		System.out.println("=============================Coprocessor===============================");
//		System.out.println("***********Long Analysis*by time****callTimeSlot4Stations**********************");
//		for(String test : s100_by_time){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4Stations(test);
//			System.out.println();
//		}	
//		
//
//		
//		System.out.println("***********Long Analysis by station******callTimeSlot4Stations**********************");
//		for(String test : s200_by_time){
//			System.out.println("====Start========="+test+"==============");
//			callTimeSlot4Stations(test);
//			System.out.println();
//		}		
//		
		System.out.println("****************callTimeStamp4PointScan*******************************");
		for(String test : locations){
			System.out.println("====Start========="+test+"==============");
			callTimeStamp4PointScan(test);
			System.out.println();		
		}		
		
		System.out.println("******************callTimeStamp4Point**********************************");
		for(String test : locations){
			System.out.println("====Start========="+test+"==============");
			callTimeStamp4Point(test);
			System.out.println();
		}
			
	}
	
	/* Private Methods */
	private String generateStation(String number){
		if(number == null) return "";		
		String station_input = "";
		boolean first = true;
		for(int i=1;i<=Integer.valueOf(number);i++){
					
			if (first)
			{
				first = false;
			}else{
				station_input += "#";
			}
			if(i<10){
				station_input += "0"+String.valueOf(i);
			}else{
				station_input += String.valueOf(i);
			}			 
					
		}		
		return station_input;
	}
	private void callTimeSlot4Stations(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");
		String start = convertDate(args[0]);
		String end = convertDate(args[1]);
		String stations = generateStation(args[2]);		
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			bixiQuery.queryAvgUsageByTimeSlot4Stations(start, end, stations);			
		}
		//System.out.println("########################################################");	
	}
	private void callTimeSlot4StationsScan(String propertyName){
		String property = tests.getProperty(propertyName);		
		String[] args = property.split(" ");
		String start = convertDate(args[0]);
		String end = convertDate(args[1]);
		String stations = generateStation(args[2]);			
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			bixiQuery.queryAvgUsageByTimeSlot4StationsWithScan(start, end, stations);
			
		}
		//System.out.println("########################################################");
	}
	private void callTimeStamp4Point(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");
		String timestamp = convertDate(args[0]);
		Double latitude = Double.parseDouble(args[1]);
		Double longitude = Double.parseDouble(args[2]);	
		Double radius = Double.parseDouble(args[3]);		
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			bixiQuery.queryAvailableByTimeStamp4Point(timestamp, latitude, longitude, radius);			
		}	
		//System.out.println("########################################################");
	}
	private void callTimeStamp4PointScan(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");
		String timestamp = convertDate(args[0]);
		Double latitude = Double.parseDouble(args[1]);
		Double longitude = Double.parseDouble(args[2]);
		Double radius = Double.parseDouble(args[3]);
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			bixiQuery.queryAvailableByTimeStamp4PointWithScan(timestamp, latitude, longitude, radius);			
		}		
		//System.out.println("########################################################");
	}
	
	/**Location query 1 ***/
	private void callScanQueryAvailable(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);
		Double radius = Double.parseDouble(args[2]);
		QueryAbstraction locationQuery = this.getBixiLocationQuery();
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			locationQuery.scanQueryAvailableNear("", latitude, longitude, radius);			
		}		
	}
	
	private void callCopQueryAvailable(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);
		Double radius = Double.parseDouble(args[2]);
		QueryAbstraction locationQuery = this.getBixiLocationQuery();
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			locationQuery.copQueryAvailableNear("", latitude, longitude, radius);			
		}		
	}
	/**Location query 2 ***/
	private void callScanQueryPoint(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);		
		QueryAbstraction locationQuery = this.getBixiLocationQuery();
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			locationQuery.scanQueryPoint(latitude, longitude);			
		}
	}
	private void callCopQueryPoint(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);		
		QueryAbstraction locationQuery = this.getBixiLocationQuery();
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			locationQuery.copQueryPoint(latitude, longitude);			
		}		
	}
	
	/**Location query 3 ***/
	private void callScanQueryKNN(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);
		int k = Integer.parseInt(args[2]);	
		QueryAbstraction locationQuery = this.getBixiLocationQuery();
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			locationQuery.scanQueryAvailableKNN("", latitude, longitude, k);			
		}
	}
	private void callCopQueryKNN(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");		
		Double latitude = Double.parseDouble(args[0]);
		Double longitude = Double.parseDouble(args[1]);
		int k = Integer.parseInt(args[2]);	
		QueryAbstraction locationQuery = this.getBixiLocationQuery();
		for(int i=0;i<times;i++){
			System.out.println("~~~ "+i+" Time ~~");
			locationQuery.copQueryAvailableKNN("", latitude, longitude, k);		
		}		
	}	
	
	
	/***********************************************************************************
	 * ***********Because execution time of scan descrease much when it run in program
	 * **************So need to provide the batch run**********************************
	 ***********************************************************************************/
	public void runScanByBatch(String propertyName){
		this.times = 1;
		callTimeSlot4StationsScan(propertyName);
	}
	
	public void runScan4PointByBatch(String propertyName){
		this.times = 1;
		callTimeStamp4PointScan(propertyName);
	}
	
	public void runCoprocessor4StationByBatch(String propertyName){
		this.times = 1;
		callTimeSlot4Stations(propertyName);
	}
	public void runCoprocessor4PointByBatch(String propertyName){
		this.times = 1;
		callTimeStamp4Point(propertyName);
	}
	/**
	 * Query location
	 * @param propertyName
	 */
	public void runScanQueryAvailable(String propertyName){
		this.times = 1;
		callScanQueryAvailable(propertyName);
	}
	
	public void runCopQueryAvailable(String propertyName){
		this.times = 1;
		callCopQueryAvailable(propertyName);
	}
	
	public void runScanQueryPoint(String propertyName){
		this.times = 1;
		callScanQueryPoint(propertyName);
	}
	public void runCopQueryPoint(String propertyName){
		this.times = 1;
		callCopQueryPoint(propertyName);
	}
	public void runScanQueryKNN(String propertyName){
		this.times = 1;
		callScanQueryKNN(propertyName);
	}
	public void runCopQueryKNN(String propertyName){
		this.times = 1;
		callCopQueryKNN(propertyName);
	}
	
}


/*		
System.out.println("*******Short Analysis***by time********callTimeSlot4Stations******************");
for(String test : s1_by_time){
	System.out.println("====Start========="+test+"==============");
	callTimeSlot4Stations(test);
	System.out.println("====End========="+test+"==============");
}	

System.out.println("*******Short Analysis*** by station**callTimeSlot4Stations******************");
for(String test : s10_by_time){
	System.out.println("====Start========="+test+"==============");
	callTimeSlot4Stations(test);
	System.out.println("====End========="+test+"==============");
}				
*/	