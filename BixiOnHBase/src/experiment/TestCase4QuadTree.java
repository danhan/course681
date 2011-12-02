package experiment;

import bixi.hbase.query.BixiQueryQuadTreeCluster;
import junit.framework.TestCase;

public class TestCase4QuadTree extends TestCase{
	
	BixiQueryQuadTreeCluster quad_cluster = null;

	@Override
	protected void setUp() throws Exception {
		quad_cluster = new BixiQueryQuadTreeCluster();
	}

	public void test_quad_tree_query_by_time_4_stations_coprocessor(){
		
		String start = "";
		String end = "";
		String stations= "1#2...";
		//quad_cluster.queryAvgUsageByTimeSlot4Stations(start, end, stations);
		
	}
	
	public void test_quad_tree_query_by_time_4_stations_scan(){
		
		String start = "2010100100"; // 2010120107-372
		String end =   "2010101023";
		String stations= "1#2#3";
		//quad_cluster.queryAvgUsageByTimeSlot4StationsWithScan(start, end, stations);
		
	}	
	
	public void test_quad_tree_query_4_location_coprocessor(){
		String timestamp = "";
		double latitude = 0;
		double longitude = 0;
		double radius = 0;
		//quad_cluster.queryAvailableByTimeStamp4Point(timestamp, latitude, longitude, radius);		
	}		
	
	public void test_quad_tree_query_4_location_scan(){
		String timestamp = "201010010320";
		double latitude = 45.52830025;
		double longitude = -73.526967;
		double radius = 0;
		quad_cluster.queryAvailableByTimeStamp4PointWithScan(timestamp, latitude, longitude, radius);	
	}	
	
	public void test_quad_tree_query_every_cluster_scan(){
		String timestamp = "201010010320";
		quad_cluster.queryAvailableByClusters(timestamp);
	}
	
	
}
