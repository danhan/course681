package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

public class BixiLocationQueryS13 extends BixiLocationQueryS1{
	
	public BixiLocationQueryS13(){
		this.min_size_of_subspace = BixiConstant.MIN_SIZE_OF_SUBSPACE3;
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"3";
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "BixiLocationQueryS13.stat";
	}
}
