package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

/**
 * subspace = 1
 * @author dan
 *
 */
public class BixiLocationQueryS11 extends BixiLocationQueryS1{
	
	public BixiLocationQueryS11(){
		this.min_size_of_subspace = BixiConstant.MIN_SIZE_OF_SUBSPACE1;
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"1";
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "SpaceS1-1.stat";
		this.FILE_NAME_PREFIX = "SpaceS1-1";
	}
}
