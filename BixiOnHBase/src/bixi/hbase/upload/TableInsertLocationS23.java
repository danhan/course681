package bixi.hbase.upload;

import java.io.IOException;

import util.raster.XRaster;
import bixi.hbase.query.BixiConstant;

/*
 * This schema is to debug the row, column and version: {row stride=0.001,num_of_column=1000} 
 */

public class TableInsertLocationS23 extends TableInsertLocationS2{
	
	public TableInsertLocationS23() throws IOException {
		
		this.min_size_of_height = BixiConstant.MIN_SIZE_OF_SUBSPACE3;
		this.num_of_column = BixiConstant.MAX_NUM_OF_COLUMN3;
		
		this.tableName = BixiConstant.LOCATION_TABLE_NAME_2+"3";
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
