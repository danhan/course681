package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

public class BixiLocationQueryS16 extends BixiLocationQueryS1{
	
	public BixiLocationQueryS16(){
		this.min_size_of_subspace = 4;
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"6";
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "BixiLocationQueryS16.stat";
	}
}
