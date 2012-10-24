package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

/**
 * subspace = 0.01 : height=0.01, columns = 100/10000
 * 
 * @author dan
 *
 */
public class BixiLocationQueryS22 extends BixiLocationQueryS2{
	
	public BixiLocationQueryS22(){
		this.max_num_of_column = BixiConstant.MAX_NUM_OF_COLUMN2;
		this.min_size_of_height = BixiConstant.MIN_SIZE_OF_SUBSPACE2;
		
		this.tableName = BixiConstant.LOCATION_TABLE_NAME_2+"2";
		familyName = new String[] { BixiConstant.LOCATION_FAMILY_NAME };
		try {
			this.setHBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "SpaceS2-001.stat";
		this.FILE_NAME_PREFIX = "SpaceS2-001";
	}
}
