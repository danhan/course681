package bixi.hbase.upload;

import hbase.service.HBaseUtil;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;

import bixi.dataset.collection.BixiReader;
import bixi.dataset.collection.XStation;
import bixi.hbase.query.BixiConstant;

public class TableInsertSchema4 {

		HBaseUtil hbase = null;
		
		static DecimalFormat sidFomatter = new DecimalFormat("000");
			  
	  /**
	   * @throws IOException
	   */
	  public TableInsertSchema4() throws IOException {
	    
		hbase = new HBaseUtil(HBaseConfiguration.create());
		hbase.getTableHandler(BixiConstant.TABLE_NAME_4);
		
	  }

	  public void insertXmlData(int schema,int batchRow,String fileDir) throws ParserConfigurationException {
	    
		  try{
				File dir = new File(fileDir);
				if (!dir.isDirectory()) {
					System.out.println(" dir is: " + dir.getAbsolutePath());
					System.exit(1);
				}
				String[] fileNames = dir.list();
				Arrays.sort(fileNames);
				// put filename into a hash, timstamp, and filename...for each timestamp,read all the file in the value ,and parse the value of metrics
				// Hashmap<time-base-line,list<file_name>>
				HashMap<String, List<String>> fileHash = new HashMap<String, List<String>>();

				for (String fileName : fileNames) {
					if (fileName.indexOf(".xml") < 0)
						continue;
					File f = new File(dir.getAbsoluteFile() + "/" + fileName);
					if (f.length() < 1024 * 5) { // < 5k
						System.err.println("File is corrupt!" + f.getAbsolutePath());
						continue;// erroreneous file
					}

					String[] timestampes = this.parseTimeStampToDay(fileName); // hour,minute
					if (fileHash.containsKey(timestampes[0])) {
						fileHash.get(timestampes[0]).add(fileName);
					} else {
						List<String> file_list = new LinkedList<String>();
						file_list.add(fileName);
						fileHash.put(timestampes[0], file_list);
					}
				}
				
				for(String timestamp:fileHash.keySet()){
					System.out.println(timestamp+";file=>"+fileHash.get(timestamp));
				}
				

				Iterator<String> keys = fileHash.keySet().iterator();
				int progress = fileHash.keySet().size();
				int counter = 0;
				int stations = 0;
				int file_num = 0;
				//ArrayList<Put> putList = new ArrayList<Put>();
				while (keys.hasNext()) {
					String prefix = keys.next();
					counter++;
					progress--;
					// System.out.print(counter+" detail to hour:  "+prefix);
					List<HashMap<String, String>> oneDayMetrics = this
							.parseOneDayForAll(fileHash.get(prefix), fileDir);
					HashMap<String, HashMap<String, String>> station_list = this
							.getMetricsForOneStation(oneDayMetrics);

					Iterator<String> station_iterator = station_list.keySet()
							.iterator();
					
					while (station_iterator.hasNext()) {
						String station_id = station_iterator.next();
						stations++;
						// System.out.println(counter+"||"+(row_counter++)+" station: "+station_id+"=> ");

						HashMap<String, String> minutes_map = station_list
								.get(station_id);
						Iterator<String> minutes = minutes_map.keySet().iterator();
						try {
							while (minutes.hasNext()) {
								String oneMinute = minutes.next();
								String value = minutes_map.get(oneMinute);
								//System.out.println("("+oneMinute+":"+value+");");
								Put put = this.hbase.constructRow((prefix + "-" + station_id), 
										new String[]{BixiConstant.FAMILY_NAME_DYNAMIC,BixiConstant.FAMILY_NAME_DYNAMIC},
										BixiConstant.d_metrics, Integer.valueOf(oneMinute), value.split(";"));
								this.hbase.getHTable().put(put);							
							}
						} catch (Exception e) {
							e.printStackTrace();
						}				
					}
					System.out.println("time_hour=>"+prefix+";file_num=>"+fileHash.get(prefix).size()+";stations=>"+stations+";values=>"+counter+";progress=>"+progress);
					file_num+= fileHash.get(prefix).size();
				} // end of while(keys)
				
				System.out.println("finish upload file number:" + file_num);
				
		  }catch(Exception e){
			  e.printStackTrace();
		  }finally{
			  this.hbase.closeTableHandler();
		  }		
	
		
	  }

	  private String[] parseTimeStampToDay(String filename) {
		  String[] sub_keys = new String[3];
		  if (filename != null) {
			  filename = filename.substring(0, filename.lastIndexOf('_'));
			  filename = filename.replace("__", ":");

			  StringTokenizer tokens = new StringTokenizer(filename, ":");
			  String date_str = tokens.nextToken();
			  String hours_str = tokens.nextToken();

			  StringTokenizer dates = new StringTokenizer(date_str, "_");
			  StringTokenizer hours = new StringTokenizer(hours_str, "_");

			  String day = dates.nextToken();
			  String month = dates.nextToken();
			  String year = dates.nextToken();

			  String hour = hours.nextToken();
			  String minute = hours.nextToken();

			  sub_keys[0] = (year + month + day);
			  sub_keys[1] = hour;
			  sub_keys[2] = minute; 
		  }

		  return sub_keys;
	  }	  
	  
		/**
		 * @snapShots: each element indicates one file, each file stores into
		 *             hashmap with the key value as <stationid-minutes,values>
		 * @return each element indicates one station, each stations store into
		 *         hashmap with the key value as <stationid,<minutes,values>>
		 */
		private HashMap<String, HashMap<String, String>> getMetricsForOneStation(
				List<HashMap<String, String>> oneHourMetrics) {
			HashMap<String, HashMap<String, String>> stations = null;
			try {
				if (oneHourMetrics != null) {
					stations = new HashMap<String, HashMap<String, String>>();

					for (int i = 0; i < oneHourMetrics.size(); i++) {
						HashMap<String, String> oneMinutes = oneHourMetrics.get(i);
						Iterator<String> ids = oneMinutes.keySet().iterator();

						while (ids.hasNext()) {
							String station_minute = ids.next();
							String value = oneMinutes.get(station_minute);

							StringTokenizer tokens = new StringTokenizer(
									station_minute, "-");
							String station_id = sidFomatter.format(Integer.valueOf(tokens.nextToken()));
							String minute = tokens.nextToken();

							if (stations.containsKey(station_id)) {
								stations.get(station_id).put(minute, value);
							} else {
								HashMap<String, String> time_series = new HashMap<String, String>();
								time_series.put(minute, value);
								stations.put(station_id, time_series);
							}

						}

					}

				}

			} catch (Exception e) {
				e.printStackTrace();
			}

			return stations;

		}

		
		/*
		 * filename with full path hashmap: (stationId-Minutes,metrics value) e.g.
		 * ("1-56","4,27") There are 407 members, because 1 minute has a pair of
		 * metrics value, and each file has 407 stations
		 */

		private HashMap<String, String> parseOneFile(String filename) {

			File f = new File(filename);
			HashMap<String, String> station_statistics = new HashMap<String, String>();
			String[] timestamps = this.parseTimeStampToDay(f.getName());

			try {
				BixiReader reader = new BixiReader();
				reader.parseXML(filename);
				for (int i = 0; i < reader.stationList.size(); i++) {
					XStation station = reader.stationList.get(i);
					String value = station.getNbBikes() + ";"
							+ station.getNbEmptyDocks();					
					String key = station.getId() + "-" + (Integer.valueOf(timestamps[1])*60+Integer.valueOf(timestamps[2]));
					//System.out.println("hour=>"+timestamps[1]+";minute=>"+timestamps[2]+";key=>"+key);
					
					station_statistics.put(key, value);
				}
				reader.cleanStationList();

			} catch (Exception e) {
				e.printStackTrace();
			}
			return station_statistics;

		}		
		/*
		 * List of map<stationId-minute, metrics> There are 407 members because
		 * there are 407 stations
		 */

		private List<HashMap<String, String>> parseOneDayForAll(
				List<String> file_list, String directory) {
			List<HashMap<String, String>> oneDayFiles = new LinkedList<HashMap<String, String>>();
			try {
				for (String fileName : file_list) {
					if (fileName.indexOf(".xml") < 0)
						continue;
					File f = new File(directory + "/" + fileName);
					if (f.length() < 1024 * 5) { // < 5k
						System.err
								.println("File is corrupt!" + f.getAbsolutePath());
						continue;// erroreneous file
					}
					// parse one file and get all key value for all stations in one
					// timestamp
					HashMap<String, String> oneFile = parseOneFile(directory + "/"
							+ fileName);
					oneDayFiles.add(oneFile);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
			return oneDayFiles;
		}	  
	
	
}
