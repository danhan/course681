package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

public class BixiLocationQueryS11 extends BixiLocationQueryS1{
	
	public BixiLocationQueryS11(){
		this.min_size_of_subspace = 1;
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"1";
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}	
	}
}
