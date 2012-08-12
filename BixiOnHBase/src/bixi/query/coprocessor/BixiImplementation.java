package bixi.query.coprocessor;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import bixi.dataset.collection.BixiReader;
import bixi.dataset.collection.XStation;
import bixi.hbase.query.BixiConstant;

/**
 * @author 
 */
public class BixiImplementation extends BaseEndpointCoprocessor implements
BixiProtocol {

	static final Log log = LogFactory.getLog(BixiImplementation.class);

	private static byte[] colFamily = BixiConstant.SCHEMA1_FAMILY_NAME.getBytes();
	private final static String BIXI_DELIMITER = "#";
	private final static int BIXI_DATA_LENGTH = 11;
	BixiReader reader = new BixiReader();

	/******************For Location Schema1*******************************/
	
	public List<String> copQueryNeighbor4LS1(Scan scan,double latitude,double longitude,double radius)throws IOException{
		
		long sTime = System.currentTimeMillis();
		System.out.println(sTime+": in the copQueryNeighbor4LS1....");
		/**Step1: get internalScanner***/
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion().getScanner(scan);
		List<KeyValue> keyvalues = new ArrayList<KeyValue>();
		List<String> results = new ArrayList<String>();
		boolean hasMoreResult = false;		
		Point2D.Double point = new Point2D.Double(latitude,longitude);
		/**Step2: iterate the result from the scanner**/
		int count = 0;
		int accepted = 0;
		try {
			do {
				hasMoreResult = scanner.next(keyvalues);
				if(keyvalues != null && keyvalues.size() > 0){	
					for(KeyValue kv:keyvalues){
						//System.out.println(Bytes.toString(kv.getRow())+"=>"+Bytes.toString(kv.getValue()));
						count++;
						// get the distance between this point and the given point
						XStation station = reader.getStationFromJson(Bytes.toString(kv.getValue()));
						station.setId(Bytes.toString(kv.getQualifier()));
						
						Point2D.Double resPoint = new Point2D.Double(station.getLatitude(),station.getlongitude());
						double distance = resPoint.distance(point);
						/**Step3: filter the false-positive points**/
						if(distance <= radius){						
							//System.out.println("row=>"+Bytes.toString(r.getRow()) + ";colum=>"+Bytes.toString(kv.getQualifier())+ ";station=>"+station.getId());
							results.add(station.getId());
							accepted++;
						}
							
					}
				}								
				keyvalues.clear();
				
				
			} while (hasMoreResult);
			
			long eTime = System.currentTimeMillis();
			
			System.out.println("exe_time=>"+(eTime-sTime)+";result=>"+results.size()+";count=>"+count+";accepted=>"+accepted);			
			
		} finally {
			scanner.close();
		}
						
		return results;	
	}
	
	
	/******************For Location Schema2*******************************/
	
	public List<String> copQueryNeighbor4LS2(Scan scan,double latitude,double longitude,double radius)throws IOException{
		
		long sTime = System.currentTimeMillis();
		System.out.println(sTime+": in the copQueryNeighbor4LS2....");
		/**Step1: get internalScanner***/
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion().getScanner(scan);
		List<KeyValue> keyvalues = new ArrayList<KeyValue>();
		List<String> results = new ArrayList<String>();
		boolean hasMoreResult = false;		
		Point2D.Double point = new Point2D.Double(latitude,longitude);
		
		/**Step2: iterate the scan result ***/
		int count = 0;
		int accepted = 0;
		try {
			do {
				hasMoreResult = scanner.next(keyvalues);
				if(keyvalues != null && keyvalues.size() > 0){	
											
					for(KeyValue kv:keyvalues){
						//System.out.println(Bytes.toString(kv.getRow())+"=>"+Bytes.toString(kv.getValue()));
						count++;						
						// get the distance between this point and the given point
						XStation station = reader.getStationFromJson(Bytes.toString(kv.getValue()));						
						
						Point2D.Double resPoint = new Point2D.Double(station.getLatitude(),station.getlongitude());
						double distance = resPoint.distance(point);
						/**Step3: filter the false-positive points**/
						if(distance <= radius){						
							//System.out.println("row=>"+Bytes.toString(kv.getRow()) + ";colum=>"+Bytes.toString(kv.getQualifier())+ ";station=>"+station.getId());
							results.add(station.getId());
							accepted++;
						}
							
					}
				}								
				keyvalues.clear();				
				
			} while (hasMoreResult);
			
			long eTime = System.currentTimeMillis();
			
			System.out.println("exe_time=>"+(eTime-sTime)+";result=>"+results.size()+";count=>"+count+";accepted=>"+accepted);			
			
		} finally {
			scanner.close();
		}
						
		return results;	
				
	}	
	
	
	/***********************For Schema3******************/
	@Override	

	public Map<String, TotalNum> copGetTotalUsage4S3(Scan scan) throws IOException{
		
		long start = System.currentTimeMillis();
		System.out.println(start+": in the getTotalUsage_Schema3....");
		
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion().getScanner(scan);
		List<KeyValue> keyvalues = new ArrayList<KeyValue>();
		Map<String, TotalNum> result = new HashMap<String, TotalNum>();
		boolean hasMoreResult = false;
		
		try {
			do {
				hasMoreResult = scanner.next(keyvalues);
				if(keyvalues != null && keyvalues.size() > 0){	
					String stationId = Bytes.toString(keyvalues.get(0).getRow()).split("-")[1];
					// all the key value is about the qualifier you defined in scan. So this only return number of empty docks.
					int count = 0;
					for(int i=0;i<keyvalues.size();i++){						
						//long timestamp = keyvalues.get(i).getTimestamp();						
						long value = Long.valueOf(Bytes.toString(keyvalues.get(i).getValue()));	
						//System.out.println("DEBUG: timestamp=>"+timestamp+";value=>"+value);
						if(result.containsKey(stationId)){
							TotalNum tn = result.get(stationId);
							tn.add(value);
							result.put(stationId, tn);
						}else{
							TotalNum tn = new TotalNum();
							tn.add(value);
							result.put(stationId, tn);
						}
						count++;
					}
					System.out.println("station=>"+stationId+";timestamps=>"+count);
				}				
				keyvalues.clear();
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}
		
		System.out.println("exe_time=>"+(System.currentTimeMillis()-start)+";result=>"+result.size());		
		
		return result;		
	}
	
	
	
	
	
	@Override
	public Map<String, Integer> giveAvailableBikes(long milliseconds,
			List<String> stationIds, Scan scan) throws IOException {
		// scan has set the time stamp accordingly, i.e., the start and end row of
		// the scan.

		for (String qualifier : stationIds) {
			log.debug("adding qualifier: " + qualifier);
			scan.addColumn(colFamily, qualifier.getBytes());
		}
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		List<KeyValue> res = new ArrayList<KeyValue>();
		Map<String, Integer> result = new HashMap<String, Integer>();
		boolean hasMoreResult = false;
		try {
			do {
				hasMoreResult = scanner.next(res);
				for (KeyValue kv : res) {
					//log.debug("got a kv: " + kv);
					int availBikes = getFreeBikes(kv);
					String id = Bytes.toString(kv.getQualifier());
					//log.debug("result to be added is: " + availBikes + " id: " + id);
					result.put(id, availBikes);
				}
				res.clear();
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}
		return result;
	}

	private int getFreeBikes(KeyValue kv) {
		String availBikes = processKV(kv, 9);
		//log.debug("availbikes::" + availBikes);
		try {
			return Integer
					.parseInt(availBikes.substring(availBikes.indexOf("=") + 1));
		} catch (Exception e) {
			System.err.println("Non numeric value as avail bikes!");
		}
		return 0;
	}

	private String processKV(KeyValue kv, int index) {
		if (kv == null || index > 10 || index < 0)
			return null;
		//log.debug("kv.getValue()" + Bytes.toString(kv.getValue()));
		String[] str = Bytes.toString(kv.getValue()).split(
				BixiImplementation.BIXI_DELIMITER);
		// malformed value (shouldn't had been here.
		if (str.length != BixiImplementation.BIXI_DATA_LENGTH)
			return null;
		return str[index];
	}

	@Override
	public Map<String, TotalNum> giveTotalUsage(List<String> stationIds,
			Scan scan) throws IOException {
		for (String qualifier : stationIds) {
			log.debug("adding qualifier: " + qualifier);
			String colName = Integer.toString(Integer.parseInt(qualifier));
			scan.addColumn(colFamily, colName.getBytes());
		}
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		List<KeyValue> res = new ArrayList<KeyValue>();
		Map<String, TotalNum> result = new HashMap<String, TotalNum>();
		boolean hasMoreResult = false;
		try {
			do {
				hasMoreResult = scanner.next(res);
				for (KeyValue kv : res) {
					//log.debug("got a kv: " + kv);
					long emptyDocks = getEmptyDocks(kv);
					String id = Bytes.toString(kv.getQualifier());
					TotalNum tn;
					if(result.containsKey(id)){
						tn = result.get(id);
					}else{
						tn = new TotalNum();
					}
					tn.add(emptyDocks);
					//emptyDocks = emptyDocks + (prevVal != null ? prevVal.intValue() : 0);
					//log.debug("result to be added is: " + emptyDocks + " id: " + id);
					result.put(id, tn);
				}
				res.clear();
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}
		return result;
	}

	private int getEmptyDocks(KeyValue kv) {
		String availBikes = processKV(kv, 10);
		log.debug("emptyDocks::" + availBikes);
		try {
			return Integer
					.parseInt(availBikes.substring(availBikes.indexOf("=") + 1));
		} catch (Exception e) {
			System.err.println("Non numeric value as avail bikes!");
		}
		return 0;
	}

	/**
	 * make a general method that takes a pair of lat/lon and a radius and give a
	 * boolean whether it was in or out.
	 * @throws IOException
	 */
	@Override
	public Map<String, Integer> getAvailableBikesFromAPoint(double lat,
			double lon, double radius, Get get) throws IOException {
		Result r = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
				.get(get, null);
		log.debug("r is "+r);
		log.debug(r.getMap().toString());
		Map<String, Integer> result = new HashMap<String, Integer>();
		try {
			String s = null, latStr = null, lonStr = null;
			for (KeyValue kv : r.raw()) {
				s = Bytes.toString(kv.getValue());
				log.debug("cell value is: "+s);
				String[] sArr = s.split(BIXI_DELIMITER); // array of key=value pairs
				latStr = sArr[3];
				lonStr = sArr[4];
				latStr = latStr.substring(latStr.indexOf("=")+1);
				lonStr = lonStr.substring(lonStr.indexOf("=")+1);
				log.debug("lon/lat values are: "+lonStr +"; "+latStr);
				double distance =giveDistance(java.lang.Double.parseDouble(latStr), java.lang.Double.parseDouble(lonStr),
						lat, lon)- radius; 
				log.debug("distance is : "+ distance);
				if ( distance < 0) {// add it
					result.put(sArr[0], getFreeBikes(kv));
				}				
			}
		} finally {
		}
		return result;
	}

	final static double RADIUS = 6371;

	private double giveDistance(double lat1, double lon1, double lat2, double lon2) {
		double dLon = Math.toRadians(lon1 - lon2);
		double dLat = Math.toRadians(lat1 - lat2);
		double a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(Math.toRadians(lat1))
				* Math.cos(Math.toRadians(lat2)) * Math.pow(Math.sin(dLon / 2), 2);
		double res = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double distance = RADIUS * res;
		return distance;
	}

	/* Schema 2 implementation */
	private static byte[] colFamilyStat = BixiConstant.SCHEMA2_BIKE_FAMILY_NAME.getBytes();

	@Override
	public Map<String, TotalNum> getTotalUsage_Schema2(Scan scan) throws IOException {

		//System.err.println("scanning");
		scan.addFamily(colFamilyStat);

		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		List<KeyValue> res = new ArrayList<KeyValue>();
		Map<String, TotalNum> result = new HashMap<String, TotalNum>();
		boolean hasMoreResult = false;
		try {
			do {
				hasMoreResult = scanner.next(res);
				for (KeyValue kv : res) {
					String stationId = Bytes.toString(kv.getRow()).split("-")[1];
					String value = new String(kv.getValue());
					Long usage = Long.parseLong(value.split(";")[1]);
					if(result.containsKey(stationId)){
						TotalNum tn = result.get(stationId);
						tn.add(usage);
						result.put(stationId, tn);
					}else{
						TotalNum tn = new TotalNum();
						tn.add(usage);
						result.put(stationId, tn);
					}
				}
				res.clear();
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}
		return result;
	}

	@Override
	public Map<String, Integer> getAvailableBikesFromAPoint_Schema2(Scan scan) throws IOException {
		scan.addFamily(colFamilyStat);
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		Map<String, Integer> result = new HashMap<String, Integer>();
		boolean hasMoreResult = false;
		List<KeyValue> res = new ArrayList<KeyValue>();
		try {
			do {
				hasMoreResult = scanner.next(res);
				for (KeyValue kv : res) {
					String stationId = Bytes.toString(kv.getRow()).split("-")[1];
					String value = new String(kv.getValue());
					Integer free = Integer.parseInt(value.split(";")[0]);
					result.put(stationId, free);
					/*if(result.containsKey(stationId)){
						result.put(stationId, free + result.get(stationId));
					}else{
						result.put(stationId, free);
					}*/
				}
				res.clear();
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}
		return result;
	}

	public List<String> getStationsNearPoint_Schema2(double lat, double lon) throws IOException {
		Scan scan = new Scan();
		scan.addFamily(BixiConstant.SCHEMA2_CLUSTER_FAMILY_NAME.getBytes());
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		boolean hasMoreResult = false;
		List<KeyValue> res = new ArrayList<KeyValue>();
		List<String> result = new ArrayList<String>();
		try {
			do {
				hasMoreResult = scanner.next(res);
				for (KeyValue kv : res) {
					String clusterId = Bytes.toString(kv.getRow());
					String[] parts = clusterId.split(":");
					double cLat = java.lang.Double.parseDouble(parts[0]);
					double cLon = java.lang.Double.parseDouble(parts[1]);
					double dx = java.lang.Double.parseDouble(parts[2]);
					double dy = java.lang.Double.parseDouble(parts[3]);
					double distx = lat-cLat;
					double disty = lon-cLon;
					if(distx >= 0 && distx <= dx && disty <= 0 && disty <= dy){
						//get stations in cluster
						result.add(Bytes.toString(kv.getQualifier()));
					}
				}
				res.clear();
			} while (hasMoreResult);
		} finally {
			scanner.close();
		}
		return result;
	}

}