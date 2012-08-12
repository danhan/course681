package hbase.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseUtil {

	public static final Log log = LogFactory.getLog(HBaseUtil.class);
	private Configuration conf = null;
	private HBaseAdmin admin = null;
	private HTable table = null;
	private int cacheSize = 5000;
	private boolean blockCached = true;
	
	public HBaseUtil(Configuration conf){			
		try{
			if (conf == null)
				this.conf = HBaseConfiguration.create();
			else
				this.conf = conf;
			
			this.conf.set("hbase.zookeeper.property.clientPort","2181");
			this.conf.set("hbase.client.pause", "20");
			this.conf.set("hbase.client.retries.number", "11");
			this.conf.set("hbase.ipc.client.tcpnodelay","true");
			this.conf.set("ipc.ping.interval", "60000"); // 1min
			this.conf.set("ipc.socket.timeout", "300000"); // 
			
			
			this.admin = new HBaseAdmin(this.conf);
		}catch(Exception e){
			e.printStackTrace();
			log.info(e.fillInStackTrace());
		}		
	}

	public Configuration getHBaseConfig() {
		return conf;
	}
	
	public void setScanConfig(int cacheSize,boolean blockCache){
		this.cacheSize = cacheSize;
		this.blockCached = blockCache;
	}

	
	public HTable createTable(String tableName, String[] metrics,int[] max_version) throws IOException {				
		System.out.println("create table for "+tableName);
		try{
			if (admin.tableExists(tableName)) {
				System.out.println(admin.listTables());
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}			
			HTableDescriptor td = this.createTableDescription(tableName, metrics,max_version);
			System.out.println(tableName + ": <=>table descirption : "+td.toString());
			this.admin.createTable(td);	
			//this.admin.createTable(td, ("0-").getBytes(), ("2-"+Long.MAX_VALUE).getBytes(),80);
			
		}catch(Exception e){
			e.printStackTrace();
			//log.info(e.fillInStackTrace());			
		}			
		return new HTable(conf, tableName);
	}
	
	public HTable getTableHandler(String tableName){
		try{
			table = new HTable(conf, tableName);
			table.setAutoFlush(false);	
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return table;
	}
	
	public HTable getHTable(){
		return this.table;
	}
	
	
	public void insertRow(String rowKey,String[] families, String[] qualifiers, long ts,String[] values) throws Exception{
		if(table == null)
			throw new Exception("No table handler");
		
		Put put = new Put(rowKey.getBytes());
		for(int i=0;i<families.length;i++){
			if(ts > 0){
				put.add(families[i].getBytes(), qualifiers[i].getBytes(), ts, values[i].getBytes());
			}else{
				put.add(families[i].getBytes(), qualifiers[i].getBytes(),values[i].getBytes());
			}			
		}
		table.put(put);
	}
	
	public Put constructRow(String rowKey,String[] families, String[] qualifiers, long ts,String[] values) throws Exception{
		if(table == null)
			throw new Exception("!!!!!!!!!!! No table handler");
		
		Put put = new Put(rowKey.getBytes());
		put.setWriteToWAL(false);
		for(int i=0;i<families.length;i++){
			if(ts >= 0){
				put.add(families[i].getBytes(), qualifiers[i].getBytes(), ts, values[i].getBytes());
			}else{
				put.add(families[i].getBytes(), qualifiers[i].getBytes(),values[i].getBytes());
			}			
		}		
		
		return put;
		
	}
	
	public void flushBufferedRow(ArrayList<Put> putList)throws Exception{
		long start = System.currentTimeMillis();
		try{			
			table.setAutoFlush(false);
			table.setWriteBufferSize(1024*1024*12);
			table.put(putList);	
			table.setAutoFlush(true);			
			System.out.println("flushrow=> "+putList.size()+";exe time=>"+(System.currentTimeMillis()-start));	
		}catch(Exception e){
			e.printStackTrace();
		}
				
	}
		
	public void closeTableHandler(){
		try{
			if (table != null) 
				table.close();		
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	
	public HTable updateTable(String tableName,String[] metrics,int[] max_version)throws IOException{
		//log.info("entry: "+tableName + ":"+metrics);
		try{
			
			HTableDescriptor td = this.createTableDescription(tableName, metrics,max_version);
			this.admin.disableTable(tableName);
			this.admin.modifyTable(tableName.getBytes(), td);
			this.admin.enableTable(tableName);	
			
		}catch(Exception e){
			log.info(e.fillInStackTrace());
			e.printStackTrace();
		}
		//log.info("exit");
		return new HTable(tableName);

	}
	
	public void deleteTable(String tableName)throws IOException{
		//log.info("entry: "+tableName);
		try{			
			if(this.admin.tableExists(tableName)){
				this.admin.disableTable(tableName);
				this.admin.deleteTable(tableName);
			}			
		}catch(Exception e){
			log.equals(e.fillInStackTrace());
			e.printStackTrace();
		}
		//log.info("exit");
	}
		
	//create 'mytable', {NAME=>'colfam:', COMPRESSION=>'lzo'}
	private synchronized HTableDescriptor createTableDescription(String tableName,String[] metrics,int[] max_version){
		//log.info("entry: "+tableName + ":"+metrics);
		HTableDescriptor td = new HTableDescriptor(tableName);
		try{
			for (int i = 0; i < metrics.length; i++) {				
				String colName = metrics[i];				
				if (colName==null || colName.length() == 0) {
					log.info("Invalid table schema content, contains empty name column.");
					throw new Exception("Invalid table schema content, contains empty name column.");
				}
				HColumnDescriptor hcd = new HColumnDescriptor(colName);
				hcd.setMaxVersions(max_version[i]);
				//hcd.setBloomFilterType(BloomType.ROWCOL);
				
				// compress it and require to install LZO
				//hcd.setCompressionType(Compression.Algorithm.GZ);
				td.addFamily(hcd);
			}
			//td.setMaxFileSize(1073741824);
			
		}catch(Exception e){
			//log.error(e.fillInStackTrace());
			e.printStackTrace();
		}
		
		//log.info("exit");
		return td;				
	}

	private CompareOp matchCompareOperation(String compareOp) throws Exception{
		CompareOp operator = null;
		if (compareOp == null)
			throw new Exception("the compare operation is invalid");
		
		try{
			if(compareOp.equals("=")){
				operator = CompareFilter.CompareOp.EQUAL;
			}else if(compareOp.equals(">")){
				operator = CompareFilter.CompareOp.GREATER;
			}else if(compareOp.equals("<")){
				operator = CompareFilter.CompareOp.LESS;
			}else if(compareOp.equals(">=")){
				operator = CompareFilter.CompareOp.GREATER_OR_EQUAL;
			}else if(compareOp.equals("<=")){
				operator = CompareFilter.CompareOp.LESS_OR_EQUAL;
			}else{
				throw new Exception("The compare operation: "+compareOp+" is invalid");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return operator;
	}
	
	public Filter getSubStringFilter(String compareOp, String regex) throws Exception{
		CompareOp operator = matchCompareOperation(compareOp);		
		return new RowFilter(operator,new SubstringComparator(regex));
	}
	
	public Filter getBinaryFilter(String compareOp, String rowkey) throws Exception{
		CompareOp operator = matchCompareOperation(compareOp);		
		return new RowFilter(operator,new BinaryComparator(rowkey.getBytes()));
	}
	
	
	public Filter getPrefixFilter(String prefix) throws Exception{		
		return new PrefixFilter(prefix.getBytes());
	}
	
	public Filter getRegrexRowFilter(String compareOp, String regex) throws Exception{
		CompareOp operator = matchCompareOperation(compareOp);			
		return new RowFilter(operator,new RegexStringComparator(regex));
	}	

	// Note: there is a jar file from google: google-collections-0.8.jar need to be imported.
	public Filter getTimeStampFilter(List<Long> timestamps) throws Exception{
				
		if (timestamps == null || timestamps.size()<=0)
			throw new Exception("the timestamps list is null");
		
		return new TimestampsFilter(timestamps);	
				
	}	
	/*
	 * access the first column
	 */
	public Filter getFirstColumnFilter() throws Exception{
		return new FirstKeyOnlyFilter();
	}
	
	public Filter getInclusiveFilter(String stopRow) throws Exception{
		return new InclusiveStopFilter(Bytes.toBytes(stopRow));
	}
	
	/*
	 * only access keys
	 */
	public Filter getKeyOnlyFilter() throws Exception {
		return new KeyOnlyFilter();
	}
	
	/**
	 * It should be noticed that the stop row in scan is not included as default
	 * @param rowRange
	 * @param filterList
	 * @param family
	 * @param columns
	 * @param maxVersion
	 * @return
	 * @throws Exception
	 */
	public ResultScanner getResultSet(String[] rowRange,FilterList filterList,String[] family,String[] columns,int maxVersion) throws Exception{
		if(table == null)
			throw new Exception("No table handler");
		if(cacheSize < 0)
			throw new Exception("should set cache size before scanning");
		
		Scan scan = null;
		ResultScanner rscanner = null;
		
		try{
			scan = new Scan();
			
			scan.setCaching(this.cacheSize);
			scan.setCacheBlocks(blockCached);
			scan.setFilter(filterList);	
			if(maxVersion>0)
				scan.setMaxVersions(maxVersion);
			
			// scan exclude the stop row directly, so have to make a little difference of the stop row 
			if(rowRange != null){
				scan.setStartRow(rowRange[0].getBytes());
				if(rowRange.length == 2 && rowRange[1] != null)
					scan.setStopRow((rowRange[1]).getBytes());			
			}	
			
			if(columns != null){
				for(int i=0;i<columns.length;i++){
					scan.addColumn(family[0].getBytes(),columns[i].getBytes());	
				}	
			}			

			rscanner = this.table.getScanner(scan);
			System.out.println("finsh to get the result scanner...");
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return rscanner;
		
	}
	
	public Scan generateScan(String[] rowRange,FilterList filterList,String[] family,String[] columns,int maxVersion) throws Exception{
		if(table == null)
			throw new Exception("No table handler");
		if(cacheSize < 0)
			throw new Exception("should set cache size before scanning");
		
		Scan scan = null;		
		
		try{
			scan = new Scan();
			scan.setCaching(this.cacheSize);
			scan.setCacheBlocks(this.blockCached);
			scan.setFilter(filterList);
			if(maxVersion>0)
				scan.setMaxVersions(maxVersion);
			if(rowRange != null){
				scan.setStartRow(rowRange[0].getBytes());
				if(rowRange.length == 2)
					scan.setStopRow(rowRange[1].getBytes());			
			}
				
			if(columns != null){
				for(int i=0;i<columns.length;i++){
					scan.addColumn(family[0].getBytes(),columns[i].getBytes());	
					//System.out.println(family[i]+";"+columns[i]);
				}	
			}				
			System.out.println("finish to get the result scanner...");
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return scan;
		
	}	


	public HBaseAdmin getAdmin() {
		return admin;
	}
	

}