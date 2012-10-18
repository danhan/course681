package bixi.hbase.query;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HRegionInfo;

import bixi.conf.XConfiguration;
import bixi.dataset.statistics.XConstant;

import util.log.XCSVLog;
import util.log.XStatLog;

import hbase.service.HBaseUtil;
import hbase.service.StatUtil;

public abstract class QueryAbstraction {
	
	protected HBaseUtil hbaseUtil = null;
	protected String tableName = "";
	protected String familyName[] = null;
	final int cacheSize = 5000;	
	private XStatLog statLog = null;
	private XCSVLog mainLog = null;
	private XCSVLog copLog = null;
	private XCSVLog timeLog = null;
	protected XConfiguration conf = XConfiguration.getInstance();
	public HashMap<String, HRegionInfo> regions = null;
	public List<Long> timePhase = new ArrayList<Long>();
	public StatUtil stat = null;
	
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
			this.regions = hbaseUtil.getRegions(tableName);
			this.stat = new StatUtil();
		}catch(Exception e){
			if(hbaseUtil != null)
				hbaseUtil.closeTableHandler();
			if(this.stat != null)
				this.stat.closeStat();
			e.printStackTrace();
		}
	}
	public void getStatLog(String filename){
		this.statLog = new XStatLog(filename);
	}
	
	public void writeStat(String str){
		this.statLog.write(str);
	}
	public void closeStatLog(){
		this.statLog.close();
	}
	
	/**
	 * 
	 * @param filename
	 * @param header
	 * @param n 0-main, 1-cop, 2-time
	 */
	public void getCSVLog(String filename,int n){
		if(n == 0){
			filename += "-main.csv";
			this.mainLog = new XCSVLog(filename,XConstant.main_header);
		}else if(n == 1){
			filename += "-cop.csv";
			this.copLog = new XCSVLog(filename,XConstant.cop_header);
		}else if(n == 2){
			filename += "-time.csv";
			this.timeLog = new XCSVLog(filename,XConstant.time_header);
		}
		
	}
	
	public void writeCSVLog(String str,int n){
		if(n == 0){
			this.mainLog.write(str);			
		}else if(n == 1){		
			this.copLog.write(str);
		}else if(n == 2){		
			this.timeLog.write(str);
		}
		
	}
	public void closeCSVLog(){
		if(this.mainLog != null)
			this.mainLog.close();
		if(this.copLog != null)
			this.copLog.close();
		if(this.timeLog != null)
			this.timeLog.close();
				
	}	

	public String getTableName() {
		return tableName;
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
	public abstract String copQueryPoint(double latitude, double longitude);
	/**
	 * This is for point query with scan
	 * @param latitude
	 * @param longitude
	 */
	public abstract String scanQueryPoint(double latitude, double longitude);
	
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
	public abstract TreeMap<Double,String> scanQueryAvailableKNN(String timestamp,
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

