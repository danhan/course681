package experiment;

import bixi.hbase.query.BixiQuerydFOFCluster;
import junit.framework.TestCase;

/*
 * This is for a sample , we need more test cases
 * 
 */
public class TestCase4dFOF extends TestCase{

	BixiQuerydFOFCluster dFOF = new BixiQuerydFOFCluster();
	@Override
	protected void setUp() throws Exception {
		// TODO Auto-generated method stub
		super.setUp();
	}
	
	public void test_dfof_query_by_time_4_stations_scan(){
		
		String start = "";
		String end = "";
		String stations= "1#2...";
		dFOF.queryAvgUsageByTimeSlot4StationsWithScan(start, end, stations);
		
	}	
	
	public void test_dfof_query_4_location_scan(){
		String timestamp = "";
		double latitude = 0;
		double longitude = 0;
		double radius = 0;
		dFOF.queryAvailableByTimeStamp4PointWithScan(timestamp, latitude, longitude, radius);	
	}		
	

}
