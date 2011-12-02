package experiment;

import bixi.hbase.query.BixiQuerySchema1;
import junit.framework.TestCase;

public class TestCase4Schema1 extends TestCase{

	BixiQuerySchema1 schema1 = null;
	
	@Override
	protected void setUp() throws Exception {
		schema1 = new BixiQuerySchema1();		
	}

	public void test_schema1_query_by_time_4_stations_coprocessor(){
		
		String start = "";
		String end = "";
		String stations= "1#2...";
		//schema1.queryAvgUsageByTimeSlot4Stations(start, end, stations);
	}
	
	public void test_schema1_query_by_time_4_stations__scan(){
	
		String sDateWithHour = "01_10_2010__00"; // day, month,year, hour
		String eDateWithHour = "01_10_2010__23";
		String stations= "1#2#3";
		//schema1.queryAvgUsageByTimeSlot4StationsWithScan(sDateWithHour, eDateWithHour, stations);
		
		sDateWithHour = "01_10_2010__00"; // day, month,year, hour
		eDateWithHour = "02_10_2010__23";
		stations= "1#2#3";
		//schema1.queryAvgUsageByTimeSlot4StationsWithScan(sDateWithHour, eDateWithHour, stations);
		
		sDateWithHour = "01_10_2010__00"; // day, month,year, hour
		eDateWithHour = "03_10_2010__23";
		stations= "1#2#3";
		//schema1.queryAvgUsageByTimeSlot4StationsWithScan(sDateWithHour, eDateWithHour, stations);		
	}	
	
	public void test_schema1_query_4_location_coprocessor(){
	
		String timestamp = "";
		double latitude = 0;
		double longitude = 0;
		double radius = 0;
		//schema1.queryAvailableByTimeStamp4Point(timestamp, latitude, longitude, radius);		
	}		
	
	public void test_schema1_query_4_location_scan(){
		
		String timestamp = "01_10_2010__03";
		double latitude = 45.52830025;
		double longitude = -73.526967;
		double radius = 7;
		schema1.queryAvailableByTimeStamp4PointWithScan(timestamp, latitude, longitude, radius);
	}
	

}
