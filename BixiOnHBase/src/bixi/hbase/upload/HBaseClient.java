package bixi.hbase.upload;

import hbase.service.HBaseUtil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import bixi.hbase.query.BixiConstant;

public class HBaseClient {
	
	static private Configuration conf = HBaseConfiguration.create();	
	
	public static void main(String[] args) throws IOException{
		
		//conf.addResource("/opt/hbaseMT/hbase-0.93-SNAPSHOT/hbase-site.xml");
		
		HBaseUtil hbase  = new HBaseUtil(conf);		
		
		String tablename = null;		
		if (args.length > 1) {	
			
			if("31".equals(args[0])){ // schema 3
				String[] metrics = {BixiConstant.SCHEMA2_CLUSTER_FAMILY_NAME}; 	
				hbase.createTable(BixiConstant.SCHEMA2_CLUSTER_TABLE_NAME,metrics);
				System.out.println("finish creating the table: "+BixiConstant.SCHEMA2_CLUSTER_TABLE_NAME);				
				metrics[0] = BixiConstant.SCHEMA2_BIKE_FAMILY_NAME; 				
				hbase.createTable(BixiConstant.SCHEMA2_BIKE_TABLE_NAME, metrics);	
				System.out.println("finish creating the table: "+BixiConstant.SCHEMA2_BIKE_TABLE_NAME);
			}else if("32".equals(args[0])){ // schema 3 insert the cluster table
				try{
					 System.out.println("insert the cluster....");	
					  TableInsertCluster inserter = new TableInsertCluster();
					  inserter.insertRow();	
					  System.out.println("finish....");
				}catch(Exception e){
					e.printStackTrace();
				}							
			}else if("33".equals(args[0])){
				try{
					 TableInsertStatistics inserter = new TableInsertStatistics();		
					 String fileDir = (args.length==2)? args[1]:"./data2";
					 inserter.batchInsertRow4schema3(fileDir);										
				}catch(Exception e){
					e.printStackTrace();
				}				
			}else if("1".equals(args[0])){	
				tablename = args[1];
				String[] metrics = {"Data"}; 
				System.out.println("start to create table");
				hbase.createTable(tablename, metrics);
			}else if("2".equals(args[0])){
				tablename = args[1];
				System.out.println("start to get result");
				hbase.getResult(tablename);
			}else if("3".equals(args[0])){
				tablename = args[1];
				hbase.deleteTable(tablename);
			}else if("11".equals(args[0])){
				try{
					tablename = "BixiData";
					String[] metrics = {"Data"}; 					
					hbase.createTable(tablename, metrics);
					System.out.println("finish creating the table: "+tablename);
				}catch(Exception e){
					e.printStackTrace();
				}
			}else if("12".equals(args[0])){
				try{
				    TableInsertPrev inserter = new TableInsertPrev();			    
				    inserter.insertXmlData(args[1]);					
				}catch(Exception e){
					e.printStackTrace();
				}
			}else if("21".equals(args[0])){
				tablename = "Station_Cluster";
				String[] metrics = {"stations"}; 				
				hbase.createTable(tablename, metrics);	
				System.out.println("finish creating the table: "+tablename);
			}else if("22".equals(args[0])){
				tablename = "Station_Statistics";
				String[] metrics = {"statistics"}; 
				System.out.println("start to create table");
				hbase.createTable(tablename, metrics);	
			}else if("23".equals(args[0])){ // insert cluster value
				try{
					  TableInsertCluster inserter = new TableInsertCluster();
					  inserter.insertRow();										
				}catch(Exception e){
					e.printStackTrace();
				}
			}else if("24".equals(args[0])){
				try{
					 TableInsertStatistics inserter = new TableInsertStatistics();		
					 String fileDir = (args.length==2)? args[1]:"./data2";
					 inserter.batchInsertRow(fileDir);										
				}catch(Exception e){
					e.printStackTrace();
				}
			}else if("41".equals(args[0])){
				tablename = "test1001";
				String[] metrics = {"station"}; 
				System.out.println("start to create table");
				hbase.createTable(tablename, metrics);
				tablename = BixiConstant.SCHEMA2_dFOF_CLUSTER_TABLE_NAME;
				metrics[0] = BixiConstant.SCHEMA2_dFOF_CLUSTER_FAMILY_NAME; 
				System.out.println("start to create table");
				hbase.createTable(tablename, metrics);				
			}
			
		}
		
	}
}
