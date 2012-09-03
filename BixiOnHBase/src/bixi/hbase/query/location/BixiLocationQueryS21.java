package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

/**
 * this is to investigate the number of rows impact for the performance query
 * RESULT: It is obvious that the more rows scanned the worse performance we can get
 * row stride is 0.01
 * @author dan
 * 
 */
public class BixiLocationQueryS21 extends BixiLocationQueryS2{	
	
	public BixiLocationQueryS21(){
		this.max_num_of_column = BixiConstant.MAX_NUM_OF_COLUMN;
		this.min_size_of_height = 0.01;
		
		this.tableName = BixiConstant.LOCATION_TABLE_NAME_2+"1";
		familyName = new String[] { BixiConstant.LOCATION_FAMILY_NAME };
		try {
			this.setHBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "BixiLocationQueryS21.stat";
	}
}
