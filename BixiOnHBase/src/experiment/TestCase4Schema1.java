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
		schema1.queryAvgUsageByTimeSlot4Stations(start, end, stations);
	}
	
	public void test_schema1_query_by_time_4_stations__scan(){
	
		String sDateWithHour = "";
		String eDateWithHour = "";
		String stations= "1#2...";
		schema1.queryAvgUsageByTimeSlot4StationsWithScan(sDateWithHour, eDateWithHour, stations);
	}	
	
	public void test_schema1_query_4_location_coprocessor(){
	
		String timestamp = "";
		double latitude = 0;
		double longitude = 0;
		double radius = 0;
		schema1.queryAvailableByTimeStamp4Point(timestamp, latitude, longitude, radius);		
	}		
	
	public void test_schema1_query_4_location_scan(){	
		String timestamp = "";
		double latitude = 0;
		double longitude = 0;
		double radius = 0;
		schema1.queryAvailableByTimeStamp4PointWithScan(timestamp, latitude, longitude, radius);
	}
	

}
