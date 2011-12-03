package bixi.hbase.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public abstract class BixiQueryAbstraction {
	
	protected Configuration conf = HBaseConfiguration.create();
	protected int cacheSize = 5000;
	
	String cluster_table_name = "";
	String cluster_family_name = "";
	String bike_table_name = "";
	String bike_family_name = "";	
	
	public BixiQueryAbstraction(int type){
		if(type == 1){
			this.bike_table_name = BixiConstant.SCHEMA1_TABLE_NAME;
			this.bike_family_name = BixiConstant.SCHEMA1_FAMILY_NAME;
		}else if(type == 2){
			this.cluster_table_name = BixiConstant.SCHEMA2_CLUSTER_TABLE_NAME;
			this.cluster_family_name = BixiConstant.SCHEMA2_CLUSTER_FAMILY_NAME;
			this.bike_table_name = BixiConstant.SCHEMA2_BIKE_TABLE_NAME;
			this.bike_family_name = BixiConstant.SCHEMA2_BIKE_FAMILY_NAME;			
		}else if(type ==3){
			this.cluster_table_name = BixiConstant.SCHEMA2_dFOF_CLUSTER_TABLE_NAME;
			this.cluster_family_name = BixiConstant.SCHEMA2_dFOF_CLUSTER_FAMILY_NAME;
			this.bike_table_name = BixiConstant.SCHEMA2_BIKE_TABLE_NAME;
			this.bike_family_name = BixiConstant.SCHEMA2_BIKE_FAMILY_NAME;			
		}
	}
	
	/*
	 * start time, end time, list of station, return all the value of usage
	 * information
	 */
	public abstract void queryAvgUsageByTimeSlot4Stations(String start, String end,
			String stations) ;
	
	/*
	 * start time, end time, list of station, return all the value of usage
	 * information
	 */
	public abstract void queryAvgUsageByTimeSlot4StationsWithScan(String start,
			String end, String stations);	
	
	/**
	 * Some one wants to know at this time, how many available bikes in the
	 * stations nearest to me
	 * 
	 * @param timestamp
	 *        , a point location return the nearest stations
	 */
	public abstract void queryAvailableByTimeStamp4Point(String timestamp,
			double latitude, double longitude,double radius);
	
	
	/**
	 * A scan method that will fetch the row from the RS and compute the
	 * distance of the points from the given ones.
	 * @timestamp 2011100100, detail to hour
	 * @param s
	 * @throws IOException
	 * @throws Throwable
	 */
	public abstract void queryAvailableByTimeStamp4PointWithScan(String timestamp,
			double latitude, double longitude,double radius);	
	
	
	
}
