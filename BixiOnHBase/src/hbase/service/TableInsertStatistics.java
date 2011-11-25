package hbase.service;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import bixi.dataset.collection.BixiReader;
import bixi.dataset.collection.XStation;

public class TableInsertStatistics {

	static Configuration conf = HBaseConfiguration.create();
	HTable table;
	static byte[] tableName = "Station_Statistics".getBytes();
	static byte[] idsFamily = "statistics".getBytes();

	/**
	 * @throws IOException
	 */
	public TableInsertStatistics() throws IOException {
		table = new HTable(conf, tableName);
		table.setAutoFlush(true);

	}

	public static void main(String[] args) throws IOException {
		TableInsertStatistics inserter = new TableInsertStatistics();
		String fileDir = "/home/dan/Downloads/BixiData/BixiData";
		inserter.batchInsertRow(fileDir);
	}

	public void insertRow(String fileDir) {

		File dir = new File(fileDir);
		if (!dir.isDirectory()) {
			System.out.println(" dir is: " + dir.getAbsolutePath());
			System.exit(1);
		}
		BixiReader reader = new BixiReader();
		String[] fileNames = dir.list();

		for (String fileName : fileNames) { // instantiate the file and dump it
			if (fileName.indexOf("xml") < 0)
				continue;
			System.out.println("processing file: " + dir.getAbsoluteFile()
					+ "/" + fileName);
			File f = new File(dir.getAbsoluteFile() + "/" + fileName);
			String[] timestamps = this.parseTimeStamp(f.getName());
			String rowkey = timestamps[0] + "-";

			try {
				reader.parseXML(dir.getAbsolutePath() + "/" + fileName);
				for (int i = 0; i < reader.stationList.size(); i++) {
					XStation station = reader.stationList.get(i);
					String value = station.getNbBikes() + ";"
							+ station.getNbEmptyDocks();
					Put put = new Put((rowkey + station.getId()).getBytes());
					put.add(idsFamily, timestamps[1].getBytes(),
							value.getBytes());
					table.put(put);
				}
				reader.cleanStationList();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public void batchInsertRow(String fileDir){
		  
		   File dir = new File(fileDir);
		    if (!dir.isDirectory()){
		      System.out.println(" dir is: "+dir.getAbsolutePath());
		      System.exit(1);
		    }
		    BixiReader reader = new BixiReader();
		    String[] fileNames = dir.list();
		    
		    // put filename into a hash, timstamp, and filename...
		    // for each timestamp,read all the file in the value ,and parse the value of metrics 
		    HashMap<String,List<String>> fileHash = new HashMap<String,List<String>>();
		    
		    for(String fileName: fileNames){		    	
		    	if(fileName.indexOf(".xml")<0) continue;
		    	fileName = fileName.substring(0, fileName.lastIndexOf("_"));
		    	String toHours = fileName.substring(0, fileName.lastIndexOf("_"));		    	
		    	if (fileHash.containsKey(toHours)){
		    		fileHash.get(toHours).add(fileName);	
		    	}else{
		    		List<String> file_list = new LinkedList<String>();
		    		file_list.add(fileName);
		    		fileHash.put(toHours, file_list);
		    	}		    	
		    }
		    
		    TreeMap<String,List<String>> sorted = new TreeMap<String,List<String>>(fileHash);
		    Iterator<String> keys = sorted.keySet().iterator();
		    int counter = 0;
		    while(keys.hasNext()){
		    	String prefix = keys.next();
		    	System.out.print((counter++)+"  == "+prefix+" : ( ");
				Iterator<String> stamps = sorted.get(prefix).iterator();
				int index = 0;
				//while(stamps.hasNext()){
				//	System.out.print(stamps.next()+";");
				//	index++;
				//}		    	
		    	System.out.println(")=="+index);
		    }
		    
		    
	}
		    
		    
	private String[] parseTimeStamp(String filename) {
		String[] sub_keys = {};
		if (filename != null) {
			filename = filename.substring(0, filename.lastIndexOf('_'));
			filename = filename.replace("__", ":");
			System.out.println(filename);
			StringTokenizer tokens = new StringTokenizer(filename, ":");
			String date_str = tokens.nextToken();
			String hours_str = tokens.nextToken();
			System.out.println(date_str + ", " + hours_str);
			StringTokenizer dates = new StringTokenizer(date_str, "_");
			StringTokenizer hours = new StringTokenizer(hours_str, "_");

			String day = dates.nextToken();
			String month = dates.nextToken();
			String year = dates.nextToken();

			String hour = hours.nextToken();
			String minute = hours.nextToken();

			sub_keys[0] = (year + month + day + hour);
			sub_keys[1] = minute;
		}

		return sub_keys;
	}
}
