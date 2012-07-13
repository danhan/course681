package bixi.hbase.upload;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;

import bixi.dataset.collection.BixiReader;

import hbase.service.HBaseUtil;

public abstract class TableInsertAbstraction {
	
	HBaseUtil hbase = null;
	protected String tableName = null;
	/**
	 * It is to use as read the file to get all location information
	 */
	BixiReader reader = null;
	
	  /**
	   * @throws IOException
	   */
	  public TableInsertAbstraction() throws IOException {
	    
		hbase = new HBaseUtil(HBaseConfiguration.create());
		reader = new BixiReader();
	  }
	  
	  /*
	   * workflow of this function:
	   * 1 parse the data 2 preprocess the data 3 insert the data into database
	   */
	  public abstract void insert();
	  
	  
}
