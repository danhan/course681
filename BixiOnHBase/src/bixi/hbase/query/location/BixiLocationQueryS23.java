package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

/**
 * row stride is 0.001
 * 
 * @author dan
 *
 */
public class BixiLocationQueryS23 extends BixiLocationQueryS2{
	
	public BixiLocationQueryS23(){
		this.max_num_of_column = BixiConstant.MAX_NUM_OF_COLUMN3;
		this.min_size_of_height = BixiConstant.MIN_SIZE_OF_SUBSPACE3;
		
		this.tableName = BixiConstant.LOCATION_TABLE_NAME_2+"3";
		familyName = new String[] { BixiConstant.LOCATION_FAMILY_NAME };
		try {
			this.setHBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "BixiLocationQueryS23.stat";
	}
}
