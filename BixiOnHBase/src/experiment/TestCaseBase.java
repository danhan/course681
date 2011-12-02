package experiment;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import bixi.hbase.query.BixiQueryAbstraction;
import junit.framework.TestCase;

public abstract class TestCaseBase extends TestCase {
	
	private static final double RADIUS = 100;
	
	BixiQueryAbstraction bixiQuery;
	
	abstract BixiQueryAbstraction getBixiQuery();
	protected String convertDate(String a){
		return a;
	}

	@Override
	protected void setUp() throws Exception {
		bixiQuery = getBixiQuery();
	}
	
	private BufferedReader readFile(String name){
		FileInputStream fstream;
		try {
			fstream = new FileInputStream(name);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		return br;
	}

	public void test_quad_tree_query_by_time_4_stations_coprocessor() throws IOException{
		
		BufferedReader br = readFile("src/experiment/query_by_time_stations.txt");
		String line;
		while ((line = br.readLine()) != null)   {
			String[] args = line.split(" ");
			String start = args[0];
			String end = args[1];
			String stations = args[2];
			bixiQuery.queryAvgUsageByTimeSlot4Stations(convertDate(start), convertDate(end), stations);
		}

	}
	
	public void test_quad_tree_query_by_time_4_stations_scan() throws IOException{
		
		BufferedReader br = readFile("src/experiment/query_by_time_stations.txt");
		String line;
		while ((line = br.readLine()) != null)   {
			String[] args = line.split(" ");
			String start = args[0];
			String end = args[1];
			String stations = args[2];
			bixiQuery.queryAvgUsageByTimeSlot4StationsWithScan(convertDate(start), convertDate(end), stations);
		}
		
	}	
	
	public void test_quad_tree_query_4_location_coprocessor() throws NumberFormatException, IOException{
		BufferedReader br = readFile("src/experiment/query_4_location.txt");
		String line;
		while ((line = br.readLine()) != null)   {
			String[] args = line.split(" ");
			String timestamp = args[0];
			Double latitude = Double.parseDouble(args[1]);
			Double longitude = Double.parseDouble(args[2]);
			bixiQuery.queryAvailableByTimeStamp4PointWithScan(convertDate(timestamp), latitude, longitude, RADIUS);
		}	
	}		
	
	public void test_quad_tree_query_4_location_scan() throws IOException{
		BufferedReader br = readFile("src/experiment/query_4_location.txt");
		String line;
		while ((line = br.readLine()) != null)   {
			String[] args = line.split(" ");
			String timestamp = args[0];
			Double latitude = Double.parseDouble(args[1]);
			Double longitude = Double.parseDouble(args[2]);
			bixiQuery.queryAvailableByTimeStamp4PointWithScan(convertDate(timestamp), latitude, longitude, RADIUS);
		}
	}	
	
}
