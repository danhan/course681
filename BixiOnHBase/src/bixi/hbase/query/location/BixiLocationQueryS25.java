package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

/**
 * row stride is 0.001
 * 
 * @author dan
 *
 */
public class BixiLocationQueryS25 extends BixiLocationQueryS2{
	
	public BixiLocationQueryS25(){
		this.max_num_of_column = BixiConstant.MAX_NUM_OF_COLUMN;;
		this.min_size_of_height = 10;
		
		this.tableName = BixiConstant.LOCATION_TABLE_NAME_2+"5";
		familyName = new String[] { BixiConstant.LOCATION_FAMILY_NAME };
		try {
			this.setHBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "BixiLocationQueryS25.stat";
	}
}
