package hbase.service;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseClient {
	
	static private Configuration conf = HBaseConfiguration.create();	
	
	public static void main(String[] args) throws IOException{
		
		//conf.addResource("/opt/hbaseMT/hbase-0.93-SNAPSHOT/hbase-site.xml");
		
		HBaseUtil hbase  = new HBaseUtil(conf);		
		
		String tablename = null;		
		if (args.length == 2) {			
			if("1".equals(args[0])){	
				tablename = args[1];
				String[] metrics = {"Data"}; 
				System.out.println("start to create table");
				hbase.createTable(tablename, metrics);
			}else if("2".equals(args[0])){
				tablename = args[1];
				System.out.println("start to get result");
				hbase.getResult(tablename);
			}else if("3".equals(args[0])){
				try{
				    DataInserter inserter = new DataInserter();			    
				    inserter.insertXmlData(args[1]);					
				}catch(Exception e){
					e.printStackTrace();
				}
			}else if("4".equals(args[0])){
				tablename = args[1];
				hbase.deleteTable(tablename);
			}
		}
		
	}
}
