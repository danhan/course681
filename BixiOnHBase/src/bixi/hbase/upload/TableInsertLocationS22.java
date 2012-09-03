package bixi.hbase.upload;

import java.io.IOException;

import util.raster.XRaster;
import bixi.hbase.query.BixiConstant;

/*
 * This schema is to debug the row, column and version: {row stride=0.001,num_of_column=1000} 
 */

public class TableInsertLocationS22 extends TableInsertLocationS2{
	
	public TableInsertLocationS22() throws IOException {
		
		this.min_size_of_height = 0.001;
		this.num_of_column = BixiConstant.MAX_NUM_OF_COLUMN;
		
		this.tableName = BixiConstant.LOCATION_TABLE_NAME_2+"2";
		this.familyName = BixiConstant.LOCATION_FAMILY_NAME;
		try{
			this.setHBase();	
		}catch(Exception e){
			e.printStackTrace();
		}
		// build the Raster for the space
		this.raster = new XRaster(space, min_size_of_height, num_of_column);		
	}

}
