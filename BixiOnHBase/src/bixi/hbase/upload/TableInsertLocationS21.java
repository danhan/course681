package bixi.hbase.upload;

import java.io.IOException;

import util.raster.XRaster;
import bixi.hbase.query.BixiConstant;

/*
 * This schema is to debug the row, column and version: {row stride=0.01,num_of_column=100} 
 */

public class TableInsertLocationS21 extends TableInsertLocationS2{
	
	public TableInsertLocationS21() throws IOException {
		
		this.min_size_of_height = 0.01;
		this.num_of_column = 1000;
		
		this.tableName = BixiConstant.LOCATION_TABLE_NAME_2+"1";
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
