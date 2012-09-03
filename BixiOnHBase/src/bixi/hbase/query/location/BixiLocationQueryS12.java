package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

public class BixiLocationQueryS12 extends BixiLocationQueryS1{
	
	public BixiLocationQueryS12(){
		this.min_size_of_subspace = 2;
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"2";
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "BixiLocationQueryS12.stat";
	}
}
