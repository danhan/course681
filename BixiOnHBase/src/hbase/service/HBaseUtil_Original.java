package hbase.service;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseUtil_Original {

	public static final Log log = LogFactory.getLog(HBaseUtil_Original.class);
	private Configuration conf = null;
	private HBaseAdmin admin = null;
	
	public HBaseUtil_Original(Configuration conf){			
		try{
			this.conf = conf;
			conf.set("hbase.zookeeper.property.clientPort","2181");
			
			this.admin = new HBaseAdmin(this.conf);
		}catch(Exception e){
			e.printStackTrace();
			log.info(e.fillInStackTrace());
		}		
	}

	public Configuration getConfig() {
		return conf;
	}

	public HTable createTable(String tableName, String[] metrics) throws IOException {				
		System.out.println("create table for "+tableName);
		try{
			if (admin.tableExists(tableName)) {
				System.out.println(admin.listTables());
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}			
			HTableDescriptor td = this.createTableDescription(tableName, metrics);
			System.out.println(tableName + ": <=>table descirption : "+td.toString());
			this.admin.createTable(td);			
		}catch(Exception e){
			e.printStackTrace();
			//log.info(e.fillInStackTrace());			
		}			
		return new HTable(conf, tableName);
	}
	
	public HTable updateTable(String tableName,String[] metrics)throws IOException{
		//log.info("entry: "+tableName + ":"+metrics);
		try{
			
			HTableDescriptor td = this.createTableDescription(tableName, metrics);
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
		
	
	private synchronized HTableDescriptor createTableDescription(String tableName,String[] metrics){
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
				hcd.setMaxVersions(1);
				td.addFamily(hcd);
			}						
		}catch(Exception e){
			//log.error(e.fillInStackTrace());
			e.printStackTrace();
		}
		
		//log.info("exit");
		return td;				
	}

	
	public void getResult(String tableName) {
		System.out.println("get Result from table name : " + tableName);
		Scan s = new Scan();
		ResultScanner ss = null;
		s.setMaxVersions();
		try {
			HTable table = new HTable(conf, tableName);
			
			ss = table.getScanner(s);

			System.out.println("Bixidata table description is : "
					+ table.getTableDescriptor().toString());
			for (Result r : ss) {

				System.out.print("the row is : " + new String(r.getRow())+": {");
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getFamily()) + "= "+ new String(kv.getValue())+";");
					
				}
				System.out.println("}");
			}			

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			ss.close();
		}

	}

	public HBaseAdmin getAdmin() {
		return admin;
	}
	

}