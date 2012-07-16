package bixi.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import bixi.dataset.collection.XStation;

/**
 * @deprecated
 *
 */
public class BixiQuery {

	static private Configuration conf = HBaseConfiguration.create();

	int cacheSize = 5000;
	String schema2_statistics = "station";//"Station_Statistics";
	String schema2_cluster = "Station_Cluster";
	String schema2_family = "s";
	String schema1_bixidata = "BixiData";

	public static void main(String[] args) throws IOException {
		BixiQuery query = new BixiQuery();
		
		
		String start = "2010100100";
		String end = "2010100123";
		String stations[] = { "10", "22", "33", "44", "45", "56", "47", "88",
				"99", "100" };

		String station_list = "";
		for (int i = 0; i < stations.length; i++) {
			station_list += stations[i] + "#";
		}

		query.Test_queryAvgUsageByTimeSlot4StationsWithScan("","",stations);
		
		String timestamp = "201010010320";
		String lat = "45.52830025"; // 45.4907715:-73.6499235
									// 45.52830025:-73.526967
		String longitude = "-73.526967";
		String[] test_cases = { "101","102","103","104"};//"12" };// "203","204",,"201","202",
										// // { "11", "21", "12", "22", "13",
										// "23" };
		String switcher = "21";
		String[] a = {"1"};
		int times = 1;
		for (int n = 0; n < times; n++) {
			System.out
					.println("***************************************************");
			System.out.println("*************Time: " + n
					+ " ***************************");
			System.out
					.println("***************************************************");
			for (int i = 0; i < test_cases.length; i++) {
				switcher = test_cases[i];
				System.out.println("**********************test case : "
						+ switcher);
				try {
					if ("11".equals(switcher)) {
						start = "01_10_2010__01";
						end = "01_10_2010__23";
						System.out.println("query 1: " + start + "=>" + end
								+ "; station: " + station_list);
						query.askAvgUsageByTimeSlot4Stations(start, end,
								station_list);
					} else if ("12".equals(switcher)) {
						timestamp = "01_10_2010__03";
						System.out.println("query2: " + start + "=>" + end
								+ "; station: " + station_list);
						query.askAvailableByTimeStamp4Point(timestamp, lat,
								longitude, "3");
					} else if ("13".equals(switcher)) {
						start = "01_10_2010__01";
						end = "01_10_2010__23";
						String[] all_stations = new String[408];
						for (int j = 1; j <= 407; j++) {
							all_stations[j] = String.valueOf(j);
						}
						query.askAvgUsageByTimeSlot4Stations(start, end, "All");
					} else if ("21".equals(switcher)) {
						start = "2010100101";
						end = "2010100123";
						query.queryAvgUsageByTimeSlot4Stations(start, end,
								stations);
					} else if ("22".equals(switcher)) {
						timestamp = "201010010300";
						query.queryAvailableByTimeStamp4Point(timestamp, lat,
								longitude);
					} else if ("23".equals(switcher)) {
						start = "2010100101";
						end = "2010100123";
						String[] all_stations = new String[408];
						for (int j = 1; j < 408; j++) {
							all_stations[j] = String.valueOf(j);
						}
						// query.queryAvgUsageByTimeSlot4Stations(start,
						// end,all_stations);
						query.queryAvgUsageByTimeSlot4StationsWithScan(start,
								end, all_stations);

					} else if ("101".equals(switcher)) { // 1 hour
						start = "01_10_2010__01";
						end = "01_10_2010__23";
						String[] all_stations = new String[408];
						for (int j = 1; j <= 407; j++) {
							all_stations[j] = String.valueOf(j);
						}
						//query.askAvgUsageByTimeSlot4Stations(start, end, "1");

						start = "2010100101";
						end = "2010100123";

						query.queryAvgUsageByTimeSlot4StationsWithScan(start,
								end, a );

					} else if ("102".equals(switcher)) { // 2 day
						start = "01_10_2010__01";
						end = "10_10_2010__23";
						String[] all_stations = new String[408];
						for (int j = 1; j <= 407; j++) {
							all_stations[j] = String.valueOf(j);
						}
						//query.askAvgUsageByTimeSlot4Stations(start, end, "1");

						start = "2010100101";
						end = "2010101023";

						query.queryAvgUsageByTimeSlot4StationsWithScan(start,
								end, a );

					} else if ("103".equals(switcher)) { // 3hour
						start = "01_10_2010__01";
						end = "20_10_2010__23";
						String[] all_stations = new String[408];
						for (int j = 1; j <= 407; j++) {
							all_stations[j] = String.valueOf(j);
						}
						//query.askAvgUsageByTimeSlot4Stations(start, end, "1");

						start = "2010100101";
						end = "2010102023";

						query.queryAvgUsageByTimeSlot4StationsWithScan(start,
								end, a );

					} else if ("104".equals(switcher)) { // 3hour
						start = "01_10_2010__01";
						end = "30_10_2010__23";
						String[] all_stations = new String[408];
						for (int j = 1; j <= 407; j++) {
							all_stations[j] = String.valueOf(j);
						}
						//query.askAvgUsageByTimeSlot4Stations(start, end, "1");

						start = "2010100101";
						end = "2010103023";
						
						query.queryAvgUsageByTimeSlot4StationsWithScan(start,
								end, a );

					} else if ("201".equals(switcher)) { // point

						lat = "45.52830025"; // 45.4907715:-73.6499235 ;
												// 45.52830025:-73.526967
						longitude = "-73.526967";
						timestamp = "01_10_2010__03";
						query.askAvailableByTimeStamp4Point(timestamp, lat,
								longitude, "3");

						timestamp = "201010010300";
						query.queryAvailableByTimeStamp4Point(timestamp, lat,
								longitude);
					} else if ("202".equals(switcher)) { // point

						lat = "45.4907715"; // 45.4907715:-73.6499235 ;
											// 45.52830025:-73.526967
						longitude = "-73.6499235";
						timestamp = "01_10_2010__03";
						query.askAvailableByTimeStamp4Point(timestamp, lat,
								longitude, "4");

						timestamp = "201010010300";
						query.queryAvailableByTimeStamp4Point(timestamp, lat,
								longitude);
					} else if ("203".equals(switcher)) { // all cluster

						lat = "45.4907715"; // 45.4907715:-73.6499235 ;
											// 45.52830025:-73.526967
						longitude = "-73.6499235";

						timestamp = "201010010300";
						query.queryAvailableByTimeStamp4Point(timestamp, lat,
								longitude);

						lat = "45.4907715"; // 45.4907715:-73.6499235 ;
											// 45.52830025:-73.526967
						longitude = "-73.6499235";
						query.queryAvailableByTimeStamp4Point(timestamp, lat,
								longitude);

						lat = "45.52830025"; // 45.4907715:-73.6499235 ;
												// 45.52830025:-73.526967
						longitude = "-73.526967";
						query.queryAvailableByTimeStamp4Point(timestamp, lat,
								longitude);

						lat = "45.52830025"; // 45.4907715:-73.6499235 ;
												// 45.52830025:-73.526967
						longitude = "-73.6499235";
						query.queryAvailableByTimeStamp4Point(timestamp, lat,
								longitude);

					} else if ("204".equals(switcher)) {
						lat = "45.4907715"; // 45.4907715:-73.6499235 ;
											// 45.52830025:-73.526967
						longitude = "-73.6499235";
						timestamp = "01_10_2010__03";
						query.askAvailableByTimeStamp4Point(timestamp, lat,
								longitude, "4");

						lat = "45.4907715"; // 45.4907715:-73.6499235 ;
											// 45.52830025:-73.526967
						longitude = "-73.6499235";
						timestamp = "01_10_2010__03";
						query.askAvailableByTimeStamp4Point(timestamp, lat,
								longitude, "4");

						lat = "45.52830025"; // 45.4907715:-73.6499235 ;
												// 45.52830025:-73.526967
						longitude = "-73.526967";
						timestamp = "01_10_2010__03";
						query.askAvailableByTimeStamp4Point(timestamp, lat,
								longitude, "4");
					} else if ("205".equals(switcher)) {
						lat = "45.52830025"; // 45.4907715:-73.6499235 ;
												// 45.52830025:-73.526967
						longitude = "-73.526967";
						timestamp = "201010010300";
						query.queryAvailableByClusters(timestamp, lat,
								longitude);
					}else if("206".equals(switcher)){
						lat = "45.52830025"; // 45.4907715:-73.6499235 ;
						// 45.52830025:-73.526967
						longitude = "-73.526967";
						timestamp = "01_10_2010__03";
						query.askAvailableByTimeStamp4Point(timestamp,lat,longitude,args[0]);
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}

	}

	/*
	 * start time, end time, list of station, return all the value of usage
	 * information
	 */
	public void queryAvgUsageByTimeSlot4Stations(String start, String end,
			String[] stations) {
		int start_hour = Integer.valueOf(start).intValue();
		int end_hour = Integer.valueOf(end).intValue();

		String[] columns = new String[60];
		for (int i = 0; i < 60; i++) {
			if (i < 10) {
				columns[i] = "0" + String.valueOf(i);
			} else {
				columns[i] = String.valueOf(i);
			}
		}

		try {
			HTable table = new HTable(conf, this.schema2_statistics.getBytes());
			Map<String, Integer> result = new HashMap<String, Integer>();
			System.out.println("start: " + start_hour + "; end hour: "
					+ end_hour);

			long starttime = System.currentTimeMillis();
			if (stations != null) {

				for (int j = 0; j < stations.length; j++) {
					int counter = 0;
					int usage = 0;
					for (int i = start_hour; i < end_hour; i++) {
						String rowKey = i + "-";
						Get get = new Get(Bytes.toBytes(rowKey + stations[j]));
						Result result_row = table.get(get);

						for (int m = 0; m < columns.length; m++) {
							byte[] metrics = result_row.getValue(
									Bytes.toBytes(schema2_family),
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

					}
					if (counter > 0) {
						result.put(stations[j], (int) (usage / counter * 1.0));
					}

				}
			}

			System.out
					.println("schema 2 : queryAvgUsageByTimeSlot4Stations time : "
							+ (System.currentTimeMillis() - starttime));
			System.out.print("schema 2 avg usage are: ");
			for (Map.Entry<String, Integer> e : result.entrySet()) {
				System.out.print("(" + e.getKey() + "=>" + e.getValue() + ");");
			}
			System.out.println();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/*
	 * start time, end time, list of station, return all the value of usage
	 * information
	 */
	public void queryAvgUsageByTimeSlot4StationsWithScan(String start,
			String end, String[] stations) {
		Scan scan = new Scan();
		scan.setCaching(cacheSize);
		scan.setCacheBlocks(true);

		try {
			HTable table = new HTable(conf, this.schema2_statistics.getBytes());
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
				scan.addColumn(Bytes.toBytes(schema2_family),
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
								Bytes.toBytes(schema2_family),
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
	
	/*
	 * start time, end time, list of station, return all the value of usage
	 * information
	 */
	public void Test_queryAvgUsageByTimeSlot4StationsWithScan(String start,
			String end, String[] stations) {
		Scan scan = new Scan();
		scan.setCaching(cacheSize);
		scan.setCacheBlocks(true);
		start = "2010101022";
		end = "2010101030";

		try {
			HTable table = new HTable(conf, this.schema2_statistics.getBytes());
			Map<String, Integer> result = new HashMap<String, Integer>();

			// Arrays.sort(stations);
			String min_station = "1";// stations[0];
			String max_station = "3";// stations[stations.length];
			if (start != null && end != null) {
				scan.setStartRow((min_station + "-" + start).getBytes());
				scan.setStopRow((max_station+"-"+end).getBytes());
			}
			if(stations!=null && stations.length>0){
				String regex = "";
				boolean first = true;
				for(String sId : stations){
				if(!first)
					regex += "|";
					first = false;
					regex += sId;
				}
				Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex+"$"));
				scan.setFilter(filter);
			}
						
			System.out.println(scan.getFilter().toString());
			

			String[] columns = new String[60];
			for (int i = 0; i < 60; i++) {
				if (i < 10) {
					columns[i] = "0" + String.valueOf(i);
				} else {
					columns[i] = String.valueOf(i);
				}
			}

			for (String qualifier : columns) {
				scan.addColumn(Bytes.toBytes(schema2_family),
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
								Bytes.toBytes(schema2_family),
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

	/**
	 * Some one wants to know at this time, how many available bikes in the
	 * stations nearest to me
	 * 
	 * @param timestamp
	 *            , a point location return the nearest stations
	 */
	public void queryAvailableByTimeStamp4Point(String timestamp,
			String latitude, String longitude) {
		Scan scan = new Scan();
		scan.setCaching(cacheSize);
		scan.setCacheBlocks(true);

		try {
			HTable table = new HTable(conf, schema2_cluster.getBytes());
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
						// byte[] value = kv.getValue();
						// XStation station =
						// this.parseMetadata(station_id,Bytes.toString(value));
						// station_list.put(station_id,station);
					}
				}

			} finally {
				scanner.close();
			}
			long cluster_access = System.currentTimeMillis();
			System.out.println("cluster access time : "
					+ (cluster_access - starttime));

			// after get the list of station
			String inside_cluster = getClusterId(new ArrayList<String>(
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
	 * Some one wants to know at this time, how many available bikes in the
	 * stations nearest to me
	 * 
	 * @param timestamp
	 *            , a point location return the nearest stations
	 */
	public void queryAvailableByClusters(String timestamp, String latitude,
			String longitude) {
		Scan scan = new Scan();
		scan.setCaching(cacheSize);
		scan.setCacheBlocks(true);

		try {
			HTable table = new HTable(conf, schema2_cluster.getBytes());
			HashMap<String, List<String>> relations = new HashMap<String, List<String>>();
			HashMap<String, Integer> avaible_list = null;
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

			System.out.println("cluster size : " + relations.size());
			
			for (String cluster : relations.keySet()) {
				HashMap<String, Integer> station_avail = new HashMap<String, Integer>();
				starttime = System.currentTimeMillis();
				if (cluster != null) {
					try {
						avaible_list = this
								.queryAvailableByTimestampAndStations(
										timestamp, new ArrayList<String>(
												relations.get(cluster)));
						for (Map.Entry<String, Integer> e : avaible_list
								.entrySet()) {
							station_avail.put(e.getKey(),
									Integer.valueOf(e.getValue()).intValue());
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					System.out.println("there is no cluster it belongs to");
				}

				System.out.print("execution time : "
						+ (System.currentTimeMillis() - starttime)
						+ "; schema2: queryAvailableByTimeStamp4Point: "
						+ station_avail.size() + " available( ");
				for (Map.Entry<String, Integer> e : station_avail.entrySet()) {
					//System.out.print("(" + e.getKey() + "," + e.getValue()
						//	+ ");");
				}
				System.out.println();
			}

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
			HTable table = new HTable(conf, this.schema2_statistics.getBytes());
			String toHour = timestamp.substring(0, timestamp.length() - 2);
			String minute = timestamp.substring(timestamp.length() - 2,
					timestamp.length());
			for (int i = 0; i < stations.size(); i++) {
				String rowKey = toHour + "-" + stations.get(i);
				Get get = new Get(Bytes.toBytes(rowKey));
				Result result = table.get(get);

				byte[] value = null;
				if (result.containsColumn(Bytes.toBytes(schema2_family),
						Bytes.toBytes(minute))) {

					value = result.getValue(Bytes.toBytes(schema2_family),
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

	private XStation parseMetadata(String station_id, String metadata) {
		XStation station = null;
		try {
			if (metadata != null) {
				StringTokenizer tokenizer = new StringTokenizer(metadata, ";");
				station = new XStation();
				station.setId(station_id);
				while (tokenizer.hasMoreTokens()) {
					StringTokenizer keyvalue = new StringTokenizer(
							tokenizer.nextToken(), "=");
					String key = keyvalue.nextToken();
					String value = keyvalue.nextToken();
					if (key.contains("name")) {
						station.setName(value);
					} else if (key.contains("terminalName")) {
						station.setTerminalName(value);
					} else if (key.contains("latitude")) {
						station.setLatitude(Double.valueOf(value).doubleValue());
					} else if (key.contains("long")) {
						station.setlongitude(Double.valueOf(value)
								.doubleValue());
					} else if (key.contains("installed")) {
						station.setInstalled(Boolean.valueOf(value)
								.booleanValue());
					} else if (key.contains("locked")) {
						station.setLocked(Boolean.valueOf(value).booleanValue());
					} else if (key.contains("installDate")) {
						station.setInstallDate(Long.valueOf(value).longValue());
					} else if (key.contains("removeDate")) {
						station.setRemoveDate(Long.valueOf(value).longValue());
					} else if (key.contains("temporary")) {
						station.setTemporary(Boolean.valueOf(value)
								.booleanValue());
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return station;

	}

	/**********************************************************************************/
	/************************************ Schema 1 **************************************/
	/**********************************************************************************/

/**
   * s shd have: [12, sDate<10_10_2010__12>, eDate<10_10_2010__15>, scan-batch size<60>, list of
   * ids<12#123#>,]
   * @param s
   * @throws IOException
   */
	public void askAvgUsageByTimeSlot4Stations(String sDateWithHour,
			String eDateWithHour, String stations) throws IOException {

		List<String> stationIds = new ArrayList<String>();

		if (!("All").equals(stations)) {
			String[] idStr = stations.split(BixiConstant.ID_DELIMITER);
			for (String id : idStr) {
				stationIds.add(id);
			}
		}
		Scan scan = new Scan();
		scan.setCaching(this.cacheSize);
		if (sDateWithHour != null && eDateWithHour != null) {
			scan.setStartRow((sDateWithHour + "_00").getBytes());
			scan.setStopRow((eDateWithHour + "_59").getBytes());
		}

		for (String qualifier : stationIds) {
			scan.addColumn(BixiConstant.SCHEMA1_FAMILY_NAME.getBytes(), qualifier.getBytes());
		}
		HTable table = new HTable(conf, schema1_bixidata.getBytes());
		Map<String, Integer> result = new HashMap<String, Integer>();

		long starttime = System.currentTimeMillis();
		ResultScanner scanner = table.getScanner(scan);
		int counter = 0;
		try {

			for (Result r : scanner) {
				// System.out.println("Row number:"+counter);
				for (KeyValue kv : r.raw()) {
					int emptyDocks = getEmptyDocks(kv);
					String id = Bytes.toString(kv.getQualifier());
					Integer prevVal = result.get(id);
					emptyDocks = emptyDocks
							+ (prevVal != null ? prevVal.intValue() : 0);

					result.put(id, emptyDocks);
					counter++;
				}
			}
		} finally {
			scanner.close();
		}

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

	/**
	 * A scan method that will fetch the row from the RS and compute the
	 * distance of the points from the given ones.
	 * 
	 * @param s
	 * @throws IOException
	 * @throws Throwable
	 */
	public void askAvailableByTimeStamp4Point(String timestamp,
			String latitude, String longitude, String radius)
			throws IOException {

		double lat = Double.parseDouble(latitude);
		double lon = Double.parseDouble(longitude);
		double rad = Double.parseDouble(radius);
		String dateWithHr = timestamp;

		long starttime = System.currentTimeMillis();
		Get g = new Get(Bytes.toBytes(dateWithHr + "_00"));
		System.out.println(dateWithHr + "_00");
		HTable table = new HTable(conf, "BixiData".getBytes());
		Result r = table.get(g);
		Map<String, Double> result = new HashMap<String, Double>();
		// this r contains the entire row for the hr+00 min. Now compute the
		// distance and do the stuff.
		String valStr = null, latStr = null, lonStr = null;
		for (KeyValue kv : r.raw()) {
			valStr = Bytes.toString(kv.getValue());
			// log.debug("cell value is: "+s);
			String[] sArr = valStr.split(BixiConstant.ID_DELIMITER); // array of
			// key=value
			// pairs
			latStr = sArr[3];
			lonStr = sArr[4];
			latStr = latStr.substring(latStr.indexOf("=") + 1);
			lonStr = lonStr.substring(lonStr.indexOf("=") + 1);
			// log.debug("lon/lat values are: "+lonStr +"; "+latStr);
			double distance = giveDistance(Double.parseDouble(latStr),
					Double.parseDouble(lonStr), lat, lon)
					- rad;

			if (distance < 0) {// with in the distance: add it
				result.put(sArr[0], distance);
			}
		}		

		System.out.println("schema1 execution time: "+ (System.currentTimeMillis() - starttime) + "  schema1 availBikes is with Scan: " + result.size());
				//+ ":" + result);
	}

	
	public void askAvailableByTimeStamp4BatchPoint(String timestamp,
			String latitude, String longitude, String radius)
			throws IOException {
		for(int i=1;i<6;i++){
			askAvailableByTimeStamp4Point(timestamp,latitude,longitude,String.valueOf((i)));
			askAvailableByTimeStamp4Point(timestamp,latitude,longitude,String.valueOf((i+0.5)));			
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

}

/*
 * String[] columns = new String[60]; for(int i=0;i<60;i++){ columns[i] =
 * String.valueOf(i); } for (String qualifier : columns) {
 * scan.addColumn("statistics".getBytes(), qualifier.getBytes()); }
 */

/*
 * else { Scan scan = new Scan(); scan.setCaching(cacheSize);
 * scan.setCacheBlocks(true); ResultScanner scanner = table.getScanner(scan);
 * scan.setStartRow((start + "1").getBytes()); scan.setStopRow((end +
 * "407").getBytes());
 * 
 * int counter = 0; try {
 * 
 * for (Result r : scanner) { // System.out.println("Row number:"+counter); for
 * (int m = 0; m < columns.length; m++) { byte[] metrics =
 * r.getValue(Bytes.toBytes("statistics"),Bytes.toBytes(columns[m])); String
 * metrics_str = Bytes.toString(metrics); if (metrics_str != null) { usage +=
 * Integer.valueOf( metrics_str.substring(0, metrics_str.indexOf(';')))
 * .intValue(); counter++; } } byte[] value = result.getValue(
 * Bytes.toBytes("statistics"), Bytes.toBytes(minute)); if (value != null) { int
 * available = Integer.valueOf(Bytes.toString( value).substring(
 * Bytes.toString(value).indexOf(';') + 1, value.length));
 * nABikes.put(stations.get(i), new Integer(available)); }
 * 
 * } } finally { scanner.close(); }
 * 
 * }
 */