package bixi.hbase.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;



public class BixiQueryQuadTreeCluster extends BixiQueryAbstraction{
	
	int cacheSize = 5000;
	String cluster_table_name = BixiConstant.SCHEMA2_CLUSTER_TABLE_NAME;
	String cluster_family_name = BixiConstant.SCHEMA2_CLUSTER_FAMILY_NAME;
	String bike_table_name = BixiConstant.SCHEMA2_BIKE_TABLE_NAME;
	String bike_family_name = BixiConstant.SCHEMA2_BIKE_FAMILY_NAME;

	@Override
	public void queryAvgUsageByTimeSlot4Stations(String start, String end,
			String stations) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void queryAvailableByTimeStamp4Point(String timestamp,
			double latitude, double longitude, double radius) {
		// TODO Auto-generated method stub
		
	}	
	
	@Override
	public void queryAvgUsageByTimeSlot4StationsWithScan(String start,
			String end, String stations) {
		// TODO split stations and add the stations to the list, filter the stations
		Scan scan = new Scan();
		scan.setCaching(cacheSize);
		scan.setCacheBlocks(true);

		try {
			HTable table = new HTable(conf, this.bike_table_name.getBytes());
			Map<String, Integer> result = new HashMap<String, Integer>();

			// Arrays.sort(stations);
			String min_station = "1";// stations[0];
			String max_station = "1";// stations[stations.length];
			if (start != null && end != null) {
				scan.setStartRow((min_station + "-" + start).getBytes());
				scan.setStopRow((max_station+"-"+end).getBytes());
			}						

			String[] columns = new String[60];
			for (int i = 0; i < 60; i++) {
				if (i < 10) {
					columns[i] = "0" + String.valueOf(i);
				} else {
					columns[i] = String.valueOf(i);
				}
			}
			
			
			

			for (String qualifier : columns) {
				scan.addColumn(Bytes.toBytes(this.bike_family_name),
						qualifier.getBytes());
			}

			long starttime = System.currentTimeMillis();
			ResultScanner scanner = null;
			int row_num=0;
			try {
				scanner = table.getScanner(scan);
				
				for (Result r : scanner) {
					int counter = 0;
					int usage = 0;
					String row = Bytes.toString(r.getRow());
					String station_id = row.substring(11, row.length());
					row_num++;
					for (int m = 0; m < columns.length; m++) {
						byte[] metrics = r.getValue(
								Bytes.toBytes(this.bike_family_name),
								Bytes.toBytes(columns[m]));
						String metrics_str = Bytes.toString(metrics);
						if (metrics_str != null) {
							usage += Integer.valueOf(
									metrics_str.substring(0,
											metrics_str.indexOf(';')))
									.intValue();
							counter++;
						}
					}
					result.put(station_id, (int) (usage / (counter * 1.0)));

				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				scanner.close();
			}
			System.out
					.println("schema 2 : queryAvgUsageByTimeSlot4StationsWithScan time : "
							+ (System.currentTimeMillis() - starttime));
			System.out.print("schema 2 avg usage are: "+row_num+"");
			for (Map.Entry<String, Integer> e : result.entrySet()) {
				System.out.print("(" + e.getKey() + "=>" + e.getValue() + ");");
			}
			System.out.println();

		} catch (Exception e) {
			e.printStackTrace();
		}

		
	}

	@Override
	public void queryAvailableByTimeStamp4PointWithScan(String timestamp,
			String latitude, String longitude, String radius) {
		Scan scan = new Scan();
		scan.setCaching(cacheSize);
		scan.setCacheBlocks(true);

		try {
			HTable table = new HTable(conf, this.cluster_table_name.getBytes());
			HashMap<String, List<String>> relations = new HashMap<String, List<String>>();
			HashMap<String, Integer> avaible_list = null;
			// HashMap<String,XStation> station_list = new
			// HashMap<String,XStation>();
			long starttime = System.currentTimeMillis();
			ResultScanner scanner = table.getScanner(scan);

			try {
				for (Result r : scanner) {
					List<String> stations = new LinkedList<String>();
					String id = Bytes.toString(r.getRow());
					relations.put(id, stations);
					for (KeyValue kv : r.raw()) {
						String station_id = Bytes.toString(kv.getQualifier());
						relations.get(id).add(station_id);
					}
				}

			} finally {
				scanner.close();
			}
			long cluster_access = System.currentTimeMillis();
			System.out.println("cluster access time : "
					+ (cluster_access - starttime));

			// after get the list of station
			String inside_cluster = this.getClusterId(new ArrayList<String>(
					relations.keySet()), latitude, longitude);
			HashMap<String, Integer> station_avail = new HashMap<String, Integer>();

			starttime = System.currentTimeMillis();

			if (inside_cluster != null) {
				try {
					avaible_list = this
							.queryAvailableByTimestampAndStations(
									timestamp,
									new ArrayList<String>(relations
											.get(inside_cluster)));
					for (Map.Entry<String, Integer> e : avaible_list.entrySet()) {
						// XStation station = station_list.get(e.getKey());
						// double distance =
						// this.distance(station.getLatitude(),
						// station.getlongitude(),
						// Double.valueOf(latitude).doubleValue(),
						// Double.valueOf(longitude).doubleValue());
						station_avail.put(e.getKey(),
								Integer.valueOf(e.getValue()).intValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				System.out.println("there is no cluster it belongs to");
			}

			System.out.println("execution time : "
					+ (System.currentTimeMillis() - starttime));

			System.out.print("schema2: queryAvailableByTimeStamp4Point: "
					+ station_avail.size() + " available( ");
			for (Map.Entry<String, Integer> e : station_avail.entrySet()) {
				System.out.print("(" + e.getKey() + "," + e.getValue() + ");");
			}
			System.out.println();

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}	
	
	/**
	 * Some one wants to know at this time, how many avialable bikes in the
	 * given station
	 * 
	 * @param timestamp
	 *            a time stamp, detail to minute
	 * @param stations
	 *            a list of stations
	 */
	private HashMap<String, Integer> queryAvailableByTimestampAndStations(
			String timestamp, List<String> stations) {
		HashMap<String, Integer> nABikes = new HashMap<String, Integer>();
		try {
			HTable table = new HTable(conf, this.bike_table_name.getBytes());
			String toHour = timestamp.substring(0, timestamp.length() - 2);
			String minute = timestamp.substring(timestamp.length() - 2,
					timestamp.length());
			for (int i = 0; i < stations.size(); i++) {
				String rowKey = toHour + "-" + stations.get(i);
				Get get = new Get(Bytes.toBytes(rowKey));
				Result result = table.get(get);

				byte[] value = null;
				if (result.containsColumn(Bytes.toBytes(this.bike_family_name),
						Bytes.toBytes(minute))) {

					value = result.getValue(Bytes.toBytes(this.bike_family_name),
							Bytes.toBytes(minute));
					if (value != null) {
						int available = Integer.valueOf(Bytes.toString(value)
								.substring(
										Bytes.toString(value).indexOf(';') + 1,
										value.length));
						nABikes.put(stations.get(i), new Integer(available));
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return nABikes;

	}	

	private String getClusterId(List<String> cluster_ids, String latitude,
			String longitude) {
		String result = null;
		try {
			double x = Double.valueOf(latitude).doubleValue();
			double y = Double.valueOf(longitude).doubleValue();
			double distance = Double.MAX_VALUE;

			if (cluster_ids != null) {
				for (int i = 0; i < cluster_ids.size(); i++) {
					String cluster = cluster_ids.get(i);
					StringTokenizer tokenizer = new StringTokenizer(cluster,
							":");
					double xl = Double.valueOf(tokenizer.nextToken())
							.doubleValue();
					double yl = Double.valueOf(tokenizer.nextToken())
							.doubleValue();
					double dx = Double.valueOf(tokenizer.nextToken())
							.doubleValue();
					double dy = Double.valueOf(tokenizer.nextToken())
							.doubleValue();

					double xb = xl + dx;
					double yb = yl - dy;

					if ((x < xl) || (x > xb) || (y < yl) || (y > yb)) {
						double tmp = this.distance(x, y, (xl + xb) / 2.0,
								(yl + yb) / 2.0);
						if (tmp < distance) {
							distance = tmp;
							result = cluster;
						}
						continue;
					} else {
						result = cluster;
						break;
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		// if there is no cluster which point is in, return the nearest one or
		// null
		return result;

	}	
	
	private double distance(double x1, double y1, double x2, double y2) {
		double dx = x2 - x1; // horizontal difference
		double dy = y2 - y1; // vertical difference
		double dist = Math.sqrt(dx * dx + dy * dy); // distance using Pythagoras
													// theorem
		return dist;
	}	

}
