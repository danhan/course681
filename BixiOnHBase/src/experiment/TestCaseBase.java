package experiment;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;
import bixi.hbase.query.BixiQueryAbstraction;

public abstract class TestCaseBase extends TestCase {
	
	private static final double RADIUS = 100;
	
	BixiQueryAbstraction bixiQuery;
	Properties tests;
	
	abstract BixiQueryAbstraction getBixiQuery();
	protected String convertDate(String a){
		return a;
	}

	@Override
	protected void setUp() throws Exception {
		bixiQuery = getBixiQuery();
		tests = new Properties();
		try {
		    tests.load(new FileInputStream("src/experiment/tests.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void test_quad_tree_query_by_time_4_stations_coprocessor_1day() throws IOException{
		callTimeSlot4Stations("1day");
	}
	
	public void test_quad_tree_query_by_time_4_stations_coprocessor_10day() throws IOException{
		callTimeSlot4Stations("10day");
	}
	
	public void test_quad_tree_query_by_time_4_stations_coprocessor_20day() throws IOException{
		callTimeSlot4Stations("20day");
	}
	
	public void test_quad_tree_query_by_time_4_stations_coprocessor_40day() throws IOException{
		callTimeSlot4Stations("40day");
	}
	
	public void test_quad_tree_query_by_time_4_stations_coprocessor_60day() throws IOException{
		callTimeSlot4Stations("60day");
	}
	
	public void test_quad_tree_query_by_time_4_stations_scan_1day() throws IOException{
		callTimeSlot4StationsScan("1day");
	}
	
	public void test_quad_tree_query_by_time_4_stations_scan_10day() throws IOException{
		callTimeSlot4StationsScan("10day");
	}
	
	public void test_quad_tree_query_by_time_4_stations_scan_20day() throws IOException{
		callTimeSlot4StationsScan("20day");
	}
	
	public void test_quad_tree_query_by_time_4_stations_scan_40day() throws IOException{
		callTimeSlot4StationsScan("40day");
	}
	
	public void test_quad_tree_query_by_time_4_stations_scan_60day() throws IOException{
		callTimeSlot4StationsScan("60day");
	}
	
	public void test_quad_tree_query_4_location_coprocessor() throws NumberFormatException, IOException{
		callTimeStamp4Point("location");
	}
	
	public void test_quad_tree_query_4_location_scan() throws NumberFormatException, IOException{
		callTimeStamp4PointScan("location");
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
