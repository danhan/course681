package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

/**
 * Subspace = 0.01
 * @author dan
 *
 */
public class BixiLocationQueryS12 extends BixiLocationQueryS1{
	
	public BixiLocationQueryS12(){
		this.min_size_of_subspace = BixiConstant.MIN_SIZE_OF_SUBSPACE2;
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"2";
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "SpaceS1-001.stat";
		this.FILE_NAME_PREFIX = "SpaceS1-001";
	}
}
