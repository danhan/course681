package experiment;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import bixi.hbase.query.BixiQueryAbstraction;

public abstract class TestCaseBase {
	
	private static final double RADIUS = 100;
	
	BixiQueryAbstraction bixiQuery;
	Properties tests;
	
	abstract BixiQueryAbstraction getBixiQuery();
	protected String convertDate(String a){
		return a;
	}
	
	private static final String[] stationTests = {"1day","10day","20day","40day","60day"};
	private static final String[] pointTests = {"location"};


	public TestCaseBase() {
		bixiQuery = getBixiQuery();
		tests = new Properties();
		try {
		    tests.load(new FileInputStream("tests.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void runTests(){
		System.out.println("RUNNING SCAN query_by_time_4_stations TESTS");
		for(String test : stationTests){
			System.out.println("RUNNING TEST: "+test);
			callTimeSlot4StationsScan(test);
		}
		
		System.out.println("RUNNING COPROCESSOR query_by_time_4_stations TESTS");
		for(String test : stationTests){
			System.out.println("RUNNING TEST: "+test);
			callTimeSlot4Stations(test);
		}
		
		System.out.println("RUNNING SCAN query_4_location TESTS");
		for(String test : pointTests){
			System.out.println("RUNNING TEST: "+test);
			callTimeStamp4PointScan(test);
		}
		
		System.out.println("RUNNING COPROCESSOR query_4_location TESTS");
		for(String test : pointTests){
			System.out.println("RUNNING TEST: "+test);
			callTimeStamp4Point(test);
		}
	}
	
	/* Private Methods */
	
	private void callTimeSlot4Stations(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");
		String start = convertDate(args[0]);
		String end = convertDate(args[1]);
		String stations = args[2];;
		bixiQuery.queryAvgUsageByTimeSlot4Stations(start, end, stations);
	}
	private void callTimeSlot4StationsScan(String propertyName){
		String property = tests.getProperty(propertyName);
		String[] args = property.split(" ");
		String start = convertDate(args[0]);
		String end = convertDate(args[1]);
		String stations = args[2];
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
