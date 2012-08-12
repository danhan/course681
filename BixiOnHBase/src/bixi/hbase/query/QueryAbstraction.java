package bixi.hbase.query;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.HashMap;
import java.util.List;

import hbase.service.HBaseUtil;

public abstract class QueryAbstraction {
	
	protected HBaseUtil hbaseUtil = null;
	protected String tableName = "";
	protected String familyName[] = null;
	final int cacheSize = 5000;	
	
	/**
	 * This should be known before indexing with QuadTree.
	 */
	protected Rectangle2D.Double space = new Rectangle2D.Double(
				BixiConstant.MONTREAL_TOP_LEFT_X,
				BixiConstant.MONTREAL_TOP_LEFT_Y,
				BixiConstant.MONTREAL_AREA_WIDTH,
				BixiConstant.MONTREAL_AREA_HEIGHT);
	
	/**
	 * 
	 * @throws Exception
	 */	
	protected void setHBase() throws Exception{
		if(familyName == null)
			throw new Exception("family Name should be set first");
		if(tableName == null)
			throw new Exception("table name should be set first");	
		
		try{
			hbaseUtil = new HBaseUtil(null);
			hbaseUtil.getTableHandler(tableName);
			hbaseUtil.setScanConfig(cacheSize, true);
		}catch(Exception e){
			if(hbaseUtil != null)
				hbaseUtil.closeTableHandler();
			e.printStackTrace();
		}
	}
	
	
	/**Some one wants to know at this time, how many available bikes in the
	 * stations nearest to me
	 * 
	 * @param timestamp
	 * @param latitude
	 * @param longitude
	 * @param radius
	 */
	public abstract List<String> copQueryAvailableNear(String timestamp,
			double latitude, double longitude,double radius);
	
	
	/**
	 *  A scan method that will fetch the row from the RS and compute the
	 * distance of the points from the given ones.
	 * @param timestamp
	 * @param latitude
	 * @param longitude
	 * @param radius
	 */
	public abstract HashMap<String,String> scanQueryAvailableNear(String timestamp,
			double latitude, double longitude,double radius);	
	
	
	/**
	 * This is for point query with Coprocessor
	 * @param latitude
	 * @param longitude
	 */
	public abstract void copQueryPoint(double latitude, double longitude);
	/**
	 * This is for point query with scan
	 * @param latitude
	 * @param longitude
	 */
	public abstract void scanQueryPoint(double latitude, double longitude);
	
	/**
	 * 
	 * @param latitude
	 * @param longitude
	 * @param area
	 */
	public abstract void copQueryArea(double latitude, double longitude,int area);	
	
	/**
	 * 
	 * @param latitude
	 * @param longitude
	 * @param area : north, south, west, east
	 */
	public abstract void scanQueryArea(double latitude, double longitude, int area);
	
	
	/**A coprocessor method that will fetch KNN for the given point 
	 * 
	 * @param timestamp
	 * @param latitude
	 * @param longitude
	 * @param radius
	 */
	public abstract void copQueryAvailableKNN(String timestamp,
			double latitude, double longitude,int n);
	
	/**
	 *  A scan method that will fetch the row from the RS and compute the
	 * distance of the points from the given ones.
	 * @param timestamp
	 * @param latitude
	 * @param longitude
	 * @param radius
	 */
	public abstract void scanQueryAvailableKNN(String timestamp,
			double latitude, double longitude,int n);	

	/**
	 * For debug
	 * @param timestamp
	 * @param latitude
	 * @param longitude
	 * @param radius
	 * @return
	 */
	public abstract List<Point2D.Double> debugColumnVersion(String timestamp,
			double latitude, double longitude, double radius);
	

	
}

