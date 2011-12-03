package bixi.hbase.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public abstract class BixiQueryAbstraction {
	
	protected Configuration conf = HBaseConfiguration.create();
	protected int cacheSize = 5000;

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
