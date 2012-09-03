package bixi.hbase.query.location;

import bixi.hbase.query.BixiConstant;

public class BixiLocationQueryS15 extends BixiLocationQueryS1{
	
	public BixiLocationQueryS15(){
		this.min_size_of_subspace = 5;
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"5";
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "BixiLocationQueryS15.stat";
	}
}
