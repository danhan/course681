package bixi.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.BixiClient;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

// TODO we need get breakdown time


public class BixiQuerySchema1 extends BixiQueryAbstraction {

	public BixiQuerySchema1(int type){
		super(1);
	}
	
	/******************************************************
	 * ********************************Coprocessor*********
	 ******************************************************/
	@Override
	public void queryAvgUsageByTimeSlot4Stations(String start,
			String end, String stations) {
		// TODO // because the coprocessor is only for onehour ==> finished 
	
		List<String> stationIds = new ArrayList<String>();

		if (!("All").equals(stations)) {
			String[] idStr = stations.split(BixiConstant.ID_DELIMITER);
			for (String id : idStr) {
				stationIds.add(id);
			}
		}	

		try {
			BixiClient client = new BixiClient(conf);
			Map<String, Integer> avgusage = client.getAvgUsageForPeriod(
					stationIds, start, end);
			System.out.println("Average Usage: " + avgusage);
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}

	}

	@Override
	public void queryAvailableByTimeStamp4Point(String timestamp,
			double latitude, double longitude, double radius) {
		System.out.println("callAvailBikesFromAPoint");
		try {
			BixiClient client = new BixiClient(conf);
			Map<String, Integer> availBikesFromAPoint = client
					.getAvailableBikesFromAPoint(latitude, longitude, radius,
							timestamp);
			System.out.println("availBikes is: " + availBikesFromAPoint);
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	/******************************************************
	 * ********************************Scan and Get*********
	 ******************************************************/

	@Override
	public void queryAvgUsageByTimeSlot4StationsWithScan(String sDateWithHour,
			String eDateWithHour, String stations) {
		
		//TODO do we need to add filter on that === finished
		
		List<String> stationIds = new ArrayList<String>();

		if (!("All").equals(stations)) {
			String[] idStr = stations.split(BixiConstant.ID_DELIMITER);
			for (String id : idStr) {
				stationIds.add(id);
			}
		}
		Scan scan = new Scan();
		scan.setCaching(this.cacheSize);
		//System.out.println(sDateWithHour + "_00" + "; " + eDateWithHour + "_59");
		if (sDateWithHour != null && eDateWithHour != null) {
			scan.setStartRow((sDateWithHour + "_00").getBytes());
			scan.setStopRow((eDateWithHour + "_59").getBytes());
		}

		String regex = getFilterRegex(sDateWithHour,eDateWithHour);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new RegexStringComparator(regex));
		scan.setFilter(filter);			
		
		
		for (String qualifier : stationIds) {
			scan.addColumn(BixiConstant.SCHEMA1_FAMILY_NAME.getBytes(), qualifier.getBytes());
		}

		Map<String, Integer> result = new HashMap<String, Integer>();

		long starttime = System.currentTimeMillis();

		int counter = 0;
		ResultScanner scanner = null;
		try {
			HTable table = new HTable(conf, this.bike_table_name.getBytes());
			scanner = table.getScanner(scan);

			for (Result r : scanner) {
				// System.out.println("Row number:"+counter);
				for (KeyValue kv : r.raw()) {
					int emptyDocks = getEmptyDocks(kv);
					String id = Bytes.toString(kv.getQualifier());
					Integer prevVal = result.get(id);
					emptyDocks = emptyDocks
							+ (prevVal != null ? prevVal.intValue() : 0);

					result.put(id, emptyDocks);

				}
				counter++;
			}
			System.out.println("schema1 get data execution time: "
					+ (System.currentTimeMillis() - starttime));

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (scanner != null)
				scanner.close();
		}

		starttime = System.currentTimeMillis();
		for (Map.Entry<String, Integer> e : result.entrySet()) {
			// System.out.println("counter and value is" + counter
			// +","+e.getKey()+": " + e.getValue());
			int i = e.getValue() / counter;
			result.put(e.getKey(), i);
		}
		System.out.println("schema1 counter: " + counter + "; time = "
				+ (System.currentTimeMillis() - starttime));
		System.out.println("schema1 Avg map is: " + result);

	}

	/*
	 * timestamp: 01_10_2010__00=> October 1st, 00:00
	 * (non-Javadoc)
	 * @see bixi.hbase.query.BixiQueryAbstraction#queryAvailableByTimeStamp4PointWithScan(java.lang.String, double, double, double)
	 */
	@Override
	public void queryAvailableByTimeStamp4PointWithScan(String timestamp,
			double latitude, double longitude, double radius) {
		// TODO change the time stamp ==> finished

		double lat = latitude;// Double.parseDouble(latitude);
		double lon = longitude; // Double.parseDouble(longitude);
		double rad = radius; // Double.parseDouble(radius);
		String dateWithHr = timestamp;
		try {
			long starttime = System.currentTimeMillis();
			Get g = new Get(Bytes.toBytes(dateWithHr + "_00"));
			HTable table = new HTable(conf, this.bike_table_name.getBytes());
			Result r = table.get(g);
			Map<String, Double> result = new HashMap<String, Double>();
			// this r contains the entire row for the hr+00 min. Now compute the
			// distance and do the stuff.
			String valStr = null, latStr = null, lonStr = null;
			for (KeyValue kv : r.raw()) {
				valStr = Bytes.toString(kv.getValue());
				// log.debug("cell value is: "+s);
				String[] sArr = valStr.split(BixiConstant.ID_DELIMITER); // array
																			// of
				// key=value
				// pairs
				latStr = sArr[3];
				lonStr = sArr[4];
				latStr = latStr.substring(latStr.indexOf("=") + 1);
				lonStr = lonStr.substring(lonStr.indexOf("=") + 1);
				// log.debug("lon/lat values are: "+lonStr +"; "+latStr);
				double distance = giveDistance(Double.parseDouble(latStr),
						Double.parseDouble(lonStr), lat, lon) - rad;

				if (distance < 0) {// with in the distance: add it
					result.put(sArr[0], distance);
				}
			}

			System.out.println("schema1 execution time: "
					+ (System.currentTimeMillis() - starttime)
					+ "  schema1 availBikes is with Scan: " + result.size());
			// + ":" + result);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private int getEmptyDocks(KeyValue kv) {
		String[] str = Bytes.toString(kv.getValue()).split(
				BixiConstant.ID_DELIMITER);
		if (str.length != 11)
			return 0;
		String availBikes = str[10];
		// System.out.println("emptyDocks::" + availBikes);
		try {
			return Integer.parseInt(availBikes.substring(availBikes
					.indexOf("=") + 1));
		} catch (Exception e) {
			System.err.println("Non numeric value as avail bikes!");
		}
		return 0;
	}

	final static double RADIUS = 6371;

	private double giveDistance(double lat1, double lon1, double lat2,
			double lon2) {
		double dLon = Math.toRadians(lon1 - lon2);
		double dLat = Math.toRadians(lat1 - lat2);
		double a = Math.pow(Math.sin(dLat / 2), 2)
				+ Math.cos(Math.toRadians(lat1))
				* Math.cos(Math.toRadians(lat2))
				* Math.pow(Math.sin(dLon / 2), 2);
		double res = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double distance = RADIUS * res;
		return distance;
	}
	/*
	 * 10 days time span as followed:
	 * start : 01_10_2010__00
	 * end:    10_10_2010__23
	 */
	
	public static String getFilterRegex(String start,String end){
		String regex = "";
		if(start !=null && end != null){
			StringTokenizer start_tokens = new StringTokenizer(start,"_");
			int s_day = Integer.valueOf(start_tokens.nextToken());
			int s_month = Integer.valueOf(start_tokens.nextToken());
			//int s_year = Integer.valueOf(start_tokens.nextToken());
			
			StringTokenizer end_tokens = new StringTokenizer(end,"_");
			int e_day = Integer.valueOf(end_tokens.nextToken());
			int e_month = Integer.valueOf(end_tokens.nextToken());
			int e_year = Integer.valueOf(end_tokens.nextToken());		
			
			if(e_month == s_month){
				boolean first = true;
				for(int i=s_day;i<=e_day;i++){
					
					if(first) 
						regex = "";
					else 
						regex += "|";
					first = false;
					
					if (i<10) 
						regex +="^0"+i;
					else  
						regex +="^"+i;
					
					regex += "_";
					regex += s_month;
					regex += "_"+e_year+"__";
					
				}
			}else {				
				if(s_month == 10){					
					if(e_month == 11){		
						boolean first = true;
						for(int i=s_day;i<=31;i++){
							if(first) 
								regex = "";
							else 
								regex += "|";
							first = false;
							
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;
							regex += "_";
							regex += s_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}
						for(int i=1;i<=e_day;i++){
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;														
							regex += "_";
							regex += e_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
						
					}else if(e_month == 12){
						boolean first = true;
						for(int i=s_day;i<=31;i++){ // October
							
							if(first) 
								regex = "";
							else 
								regex += "|";
							first = false;
														
							
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;														
							regex += "_";
							regex += s_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}
						for(int i=1;i<=31;i++){ // November
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;
							regex += "_";
							regex += (s_month+1);
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
						for(int i=1;i<=e_day;i++){ // 
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;
							regex += "_";
							regex += e_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
					}
				
				}else if(s_month == 11){
					boolean first = true;
					if(e_month == 12){
						
						if(first) 
							regex = "";
						else 
							regex += "|";
						first = false;						
						
						for(int i=s_day;i<=31;i++){ // November
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;
							regex += "_";
							regex += s_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
						for(int i=1;i<=e_day;i++){ // 
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;
							regex += "_";
							regex += e_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
					}
					
				}
				
				
			}
		//	System.out.println("regex is : "+regex);
					
		}
		
		return regex;
	}
	
	
	public static void  main(String args[]){
		String s = "";
		for(int i=1;i<=400;i++){			
			if (i<10) 
				s += "0"+i+"#";
			else
				s +=i+"#";
		}
		System.out.println(s);
		
		
		String start = "01_10_2010__00";
		String end = "10_11_2010__00";
		BixiQuerySchema1.getFilterRegex(start,end);
		start = "01_10_2010__00";
		end = "30_11_2010__00";
		BixiQuerySchema1.getFilterRegex(start,end);
	}

}
