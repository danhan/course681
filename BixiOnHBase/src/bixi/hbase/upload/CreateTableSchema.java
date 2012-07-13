package bixi.hbase.upload;

import hbase.service.HBaseUtil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import bixi.hbase.query.BixiConstant;


public class CreateTableSchema {
	static private Configuration conf = HBaseConfiguration.create();
	
	/*
	 * args indicates the schema name: 
	 * for time series data, the schema is like 3,4 (this may be changed later to t3,t4...)
	 * for location serires data, the schema is like l1,l2,l3
	 * 
	 */
	public static void main(String[] args) throws IOException {

		if(args.length < 1){
			return;
		}		
		HBaseUtil hbaseUtil = null;
		try {
			hbaseUtil = new HBaseUtil(conf);
			
			if(args[0].equals("3")){
				String families[] = {BixiConstant.FAMILY_NAME_STATIC,BixiConstant.FAMILY_NAME_DYNAMIC};
				int versions[] = {BixiConstant.FAMILY_NAME_STATIC_VERSION,BixiConstant.FAMILY_NAME_3_DYNAMIC_VERSION};
				hbaseUtil.createTable(BixiConstant.TABLE_NAME_3, families,versions);
				System.out.println("finish creating the table: " + BixiConstant.TABLE_NAME_3);				
			}else if(args[0].equals("4")){
				String families[] = {BixiConstant.FAMILY_NAME_STATIC,BixiConstant.FAMILY_NAME_DYNAMIC};
				int versions[] = {BixiConstant.FAMILY_NAME_STATIC_VERSION,BixiConstant.FAMILY_NAME_4_DYNAMIC_VERSION};
				hbaseUtil.createTable(BixiConstant.TABLE_NAME_4, families,versions);
				System.out.println("finish creating the table: " + BixiConstant.TABLE_NAME_4);				
			}else if(args[0].equals("l1")){
				String families[] = {BixiConstant.LOCATION_FAMILY_NAME};
				int versions[] = {1};
				hbaseUtil.createTable(BixiConstant.LOCATION_TABLE_NAME_1, families,versions);
				System.out.println("finish creating the table: " + BixiConstant.LOCATION_TABLE_NAME_1);				
				
			}else if(args[0].equals("l2")){
				String families[] = {BixiConstant.LOCATION_FAMILY_NAME};
				int versions[] = {1};
				hbaseUtil.createTable(BixiConstant.LOCATION_TABLE_NAME_2, families,versions);
				System.out.println("finish creating the table: " + BixiConstant.LOCATION_TABLE_NAME_2);				
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();	
		}
	}
}

