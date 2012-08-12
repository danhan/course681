package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

/**
 * This is to investigate the length of columns and versions, with the same size of rows.
 * @author dan
 *
 */
public class BixiLocationQueryS22 extends BixiLocationQueryS2{
	
	public BixiLocationQueryS22(){
		this.max_num_of_column = 10000;
		this.min_size_of_height = 0.1;
		
		this.tableName = BixiConstant.LOCATION_TABLE_NAME_2+"2";
		familyName = new String[] { BixiConstant.LOCATION_FAMILY_NAME };
		try {
			this.setHBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
