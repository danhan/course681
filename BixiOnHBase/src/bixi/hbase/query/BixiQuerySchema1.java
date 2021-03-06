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
			BixiClient client = new BixiClient(conf,1);
			Map<String, Double> avgusage = client.getAvgUsageForPeriod(
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
			BixiClient client = new BixiClient(conf,1);
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
				stationIds.add(Integer.valueOf(id).toString());
			}
		}
		Scan scan = new Scan();
		scan.setCaching(this.cacheSize);
		
		// because the rowkey , it is sored in the bigtable. So need to treat with it in this way.
		String start_point = sDateWithHour;
		String end_point = eDateWithHour;
		
		boolean in_one_month = ( Integer.valueOf(start_point.substring(3,5)) > Integer.valueOf(start_point.substring(3,5)));
		if(in_one_month){
			if(start_point.compareTo(end_point) > 0){
				start_point = eDateWithHour;
				end_point = sDateWithHour;
			}				
		}else{
			if(Integer.valueOf(start_point.substring(0,2)) > Integer.valueOf(end_point.substring(0,2))){
				start_point = eDateWithHour.replace(eDateWithHour.substring(0, 2),"01");
				
				if(start_point.compareTo(end_point) > 0){
					end_point = sDateWithHour.substring(0,sDateWithHour.length()-2)+"23";
				}else{
					end_point = eDateWithHour.substring(0,eDateWithHour.length()-2)+"23";
				}
			}
		}
			
		if (sDateWithHour != null && eDateWithHour != null) {
			scan.setStartRow((start_point + "_00").getBytes());
			scan.setStopRow((end_point + "_59"+"1").getBytes());
		}
		
		String regex = getFilterRegex(sDateWithHour,eDateWithHour);
		//System.out.println("regex is "+regex);
		
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new RegexStringComparator(regex));
		scan.setFilter(filter);			

		for (String qualifier : stationIds) {
			scan.addColumn(BixiConstant.SCHEMA1_FAMILY_NAME.getBytes(), qualifier.getBytes());
		}		
		
		
		Map<String, Integer> result = new HashMap<String, Integer>();

		long starttime = System.currentTimeMillis();

		int counter = 0;
		int row_size = 0;		
		ResultScanner scanner = null;
		try {
			HTable table = new HTable(conf, this.bike_table_name.getBytes());
			scanner = table.getScanner(scan);
			System.out.println("schema1 scan execution time: "+ (System.currentTimeMillis() - starttime));			 
			for (Result r : scanner) {
				// System.out.println("Row number:"+counter);				
				
				for (KeyValue kv : r.raw()) {
					//System.out.println(Bytes.toString(kv.getRow()));					
					if (row_size == 0) row_size = kv.getValue().length * r.raw().length;
					int emptyDocks = getEmptyDocks(kv);
					String id = Bytes.toString(kv.getQualifier());
					Integer prevVal = result.get(id);
					emptyDocks = emptyDocks
							+ (prevVal != null ? prevVal.intValue() : 0);

					result.put(id, emptyDocks);

				}
				counter++;
			}
			System.out.println("schema1: row_size=>"+row_size+"; total execution time: "+ (System.currentTimeMillis() - starttime));

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
		System.out.println("schema1 counter: " + counter + "; calculate time = "+ (System.currentTimeMillis() - starttime));
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
			System.out.println("random get time: "+(System.currentTimeMillis() - starttime));			
			
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

			System.out.println("schema1 total execution time: "
					+ (System.currentTimeMillis() - starttime)
					+ "  schema1 availBikes is with Scan: " + result.size()
			 + ":" + result);
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
			String s_day = start_tokens.nextToken();
			String s_month = start_tokens.nextToken();
			//int s_year = Integer.valueOf(start_tokens.nextToken());
			
			StringTokenizer end_tokens = new StringTokenizer(end,"_");
			String e_day = end_tokens.nextToken();
			String e_month = end_tokens.nextToken();
			String e_year = end_tokens.nextToken();		
						
			if(e_month.equals(s_month)){
				boolean first = true;
				for(int i=Integer.valueOf(s_day);i<=Integer.valueOf(e_day);i++){					
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
				if(s_month.equals("09")){	
					if(e_month.equals("10")){		
						boolean first = true;
						for(int i=Integer.valueOf(s_day);i<=30;i++){
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
						}	
						regex += "|";
						for(int i=1;i<=Integer.valueOf(e_day);i++){
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;														
							regex += "_";
							regex += e_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
					}else if(e_month.equals("11")){		
						boolean first = true;
						for(int i=Integer.valueOf(s_day);i<=31;i++){
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
						for(int i=1;i<=Integer.valueOf(e_day);i++){
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;														
							regex += "_";
							regex += e_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
						
					}else if(e_month.equals("12")){
						boolean first = true;
						for(int i=Integer.valueOf(s_day);i<=31;i++){ // October
							
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
						for(int i=1;i<=Integer.valueOf(e_day);i++){ // 
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;
							regex += "_";
							regex += e_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
					}		
				}else if(s_month.equals("10")){					
					if(e_month.equals("11")){		
						boolean first = true;
						for(int i=Integer.valueOf(s_day);i<=31;i++){
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
						for(int i=1;i<=Integer.valueOf(e_day);i++){
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;														
							regex += "_";
							regex += e_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
						
					}else if(e_month.equals("12")){
						boolean first = true;
						for(int i=Integer.valueOf(s_day);i<=31;i++){ // October
							
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
						for(int i=1;i<=Integer.valueOf(e_day);i++){ // 
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;
							regex += "_";
							regex += e_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
					}
				
				}else if(s_month.equals("11")){
					boolean first = true;
					if(e_month == "12"){						
						if(first) 
							regex = "";
						else 
							regex += "|";
						first = false;						
						
						for(int i=Integer.valueOf(s_day);i<=31;i++){ // November
							if (i<10) regex +="^0"+i;
							else  regex +="^"+i;
							regex += "_";
							regex += s_month;
							regex += "_"+e_year+"__";
							regex += "|";							
						}						
						for(int i=1;i<=Integer.valueOf(e_day);i++){ // 
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
					
		}			
		if(regex.lastIndexOf('|') == regex.length()-1)
			regex = regex.substring(0,regex.length()-2);
		//System.out.println(regex);
		return regex;
	}
	
	public static void main(String[] args){
		String s = "25_09_2010__00";
		String e = "04_10_2010__23";
		String reg = getFilterRegex(s,e);
		System.out.println(reg);
		
		String start_point = s;
		String end_point = e;
		boolean in_one_month = ( Integer.valueOf(start_point.substring(3,5)) > Integer.valueOf(start_point.substring(3,5)));
		if(in_one_month){
			System.out.println("in one month");
			if(start_point.compareTo(end_point) > 0){
				start_point = e;
				end_point = s;
			}				
		}else{
			if(Integer.valueOf(start_point.substring(0,2)) > Integer.valueOf(end_point.substring(0,2))){
				start_point = e.replace(e.substring(0, 2),"01");
				
				if(start_point.compareTo(end_point) > 0){
					end_point = s.substring(0,s.length()-2)+"23";
				}else{
					end_point = s.substring(0,s.length()-2)+"23";
				}
			}
		}
		
		
		System.out.println("tart: "+start_point + "; "+end_point);
		

	}
	

}
