package bixi.hbase.query.location;

import java.awt.geom.Point2D.Double;
import java.util.HashMap;
import java.util.List;

import bixi.hbase.query.QueryAbstraction;
/**
 * This class is to 
 * @author dan
 *
 */
public class BixiLocationQueryS3 extends QueryAbstraction{

	
	
	
	
	@Override
	public List<String> copQueryAvailableNear(String timestamp,
			double latitude, double longitude, double radius) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashMap<String, String> scanQueryAvailableNear(String timestamp,
			double latitude, double longitude, double radius) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String copQueryPoint(double latitude, double longitude) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String scanQueryPoint(double latitude, double longitude) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void copQueryArea(double latitude, double longitude, int area) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void scanQueryArea(double latitude, double longitude, int area) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void copQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void scanQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Double> debugColumnVersion(String timestamp, double latitude,
			double longitude, double radius) {
		// TODO Auto-generated method stub
		return null;
	}

	
	
	
}
