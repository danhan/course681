package bixi.hbase.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.BixiClient;

public class BixiQuerySchema3 extends BixiQueryAbstraction {

	public BixiQuerySchema3(int type){
		super(3);
	}

	@Override
	public void queryAvgUsageByTimeSlot4Stations(String start, String end,
			String stations) {
		List<String> stationIds = new ArrayList<String>();

		if (!("All").equals(stations)) {
			String[] idStr = stations.split(BixiConstant.ID_DELIMITER);
			for (String id : idStr) {
				stationIds.add(id);
			}
		}
		try{
		    BixiClient client = new BixiClient(conf,3);
		    Map<String, Double> avgusage = client
		        .copGetAvgUsageForPeriod4S3(stationIds, start, end,BixiConstant.FAMILY_NAME_3_DYNAMIC_VERSION);
		    System.out.println("Average Usage: " + avgusage);	    	
	    }catch(Exception e){
	    	e.printStackTrace();
	    }catch(Throwable e){
	    	e.printStackTrace();
	    } 	 
		
	}


	
	
	
	
	
	
	
	
	
	

	
	/*******************************Ignore the distance function***********************/
	@Override
	public void queryAvailableByTimeStamp4Point(String timestamp,
			double latitude, double longitude, double radius) {
		// TODO Auto-generated method stub
		
	}

	
	/*****************************Ignore*Scan********************************/
	
	@Override
	public void queryAvgUsageByTimeSlot4StationsWithScan(String start,
			String end, String stations) {
		// TODO Auto-generated method stub
		
	}	
	
	@Override
	public void queryAvailableByTimeStamp4PointWithScan(String timestamp,
			double latitude, double longitude, double radius) {
		// TODO Auto-generated method stub
		
	}
	
	
	

}
