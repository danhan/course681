package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

public class BixiLocationQueryS14 extends BixiLocationQueryS1{
	
	public BixiLocationQueryS14(){
		this.min_size_of_subspace = 3;
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"4";
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "BixiLocationQueryS14.stat";
	}
}
