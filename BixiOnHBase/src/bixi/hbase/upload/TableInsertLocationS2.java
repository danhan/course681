package bixi.hbase.upload;

import java.io.IOException;

import bixi.hbase.query.BixiConstant;

public class TableInsertLocationS2 extends TableInsertAbstraction{

	String tableName = BixiConstant.LOCATION_TABLE_NAME_2;
	
	public TableInsertLocationS2() throws IOException{
		super();
	}
	
	@Override
	public void insert() {
		// TODO Auto-generated method stub
		
	}

	
}
