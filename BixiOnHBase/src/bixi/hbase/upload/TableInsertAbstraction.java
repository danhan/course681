package bixi.hbase.upload;

import java.awt.geom.Rectangle2D;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;

import bixi.dataset.collection.BixiReader;
import bixi.hbase.query.BixiConstant;

import hbase.service.HBaseUtil;

public abstract class TableInsertAbstraction {
	
	HBaseUtil hbase = null;
	protected String tableName = null;
	protected String familyName = null;
	Rectangle2D.Double space = null;
	float min_size_of_subspace = 1;
	/**
	 * It is to use as read the file to get all location information
	 */
	BixiReader reader = null;
	
	  /**
	   * @throws IOException
	   */
	  public TableInsertAbstraction() throws IOException {	    	
		this.reader = new BixiReader();
		// This should be known before indexing with QuadTree.
		this.space = new Rectangle2D.Double(
				BixiConstant.MONTREAL_TOP_LEFT_X,
				BixiConstant.MONTREAL_TOP_LEFT_Y,
				BixiConstant.MONTREAL_AREA_WIDTH,
				BixiConstant.MONTREAL_AREA_HEIGHT);
		// The min size of subspace, this is based on the queries
		this.min_size_of_subspace = (float) BixiConstant.MIN_SIZE_OF_SUBSPACE;		
	  }
	  
	  public void setHBase() throws IOException{
		  hbase = new HBaseUtil(HBaseConfiguration.create());
		  hbase.getTableHandler(tableName);
	  }
	  
	  /*
	   * workflow of this function:
	   * 1 parse the data 2 preprocess the data 3 insert the data into database
	   */
	  public abstract void insert(String filename, int batchNum);
	  
	  
}
