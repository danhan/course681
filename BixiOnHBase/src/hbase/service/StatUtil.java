package hbase.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Get some statistics information for the environment
 * @author dan
 *
 */
public class StatUtil {
	
	private HBaseUtil hbaseUtil = null;
	private String metaTable = ".META.";	
	
	public StatUtil(){		
		try{			
			hbaseUtil = new HBaseUtil(null);
			hbaseUtil.getTableHandler(metaTable);					
		}catch(Exception e){
			if(hbaseUtil != null)
				hbaseUtil.closeTableHandler();
			e.printStackTrace();
		}		
	}
	
	public void closeStat(){
		if(this.hbaseUtil != null)
			this.hbaseUtil.closeTableHandler();
	}

	public List<HRegionInfo> getRegionInfoByTable(String tableName){
		
		if(tableName == null)
			return null;
		List<HRegionInfo> regionList = null;
		try{		
			regionList = hbaseUtil.getAdmin().getTableRegions(tableName.getBytes());
		}catch(Exception e){
			e.printStackTrace();
		}
			
		return regionList;
	}
	
	/**
	 * return the mapping relations between the region and region servers
	 * access the '.META': row=regionName, column = info:serverstartcode, info:regioninfo={STARTKEY=>'',ENDKEY=>''}, info:server
	 * @param tableName
	 * @return server, startkey, endkey, < HRegionInfo, server>
	 */
	public String getRSbyRegion(String tableName,String regionName){	
		String serverName = null;
		try{
						
			ResultScanner result = hbaseUtil.getResultSet(null,null,null,null,1);
			for(Result r: result){				
				String rowKey = Bytes.toString(r.getRow());				
				if(rowKey.equals(regionName)){
					List<KeyValue> pairs = r.list();				
					for(KeyValue kv:pairs){							
						if(Bytes.toString(kv.getQualifier()).equals("server")){							
							serverName = Bytes.toString(kv.getValue());
							break;
						}
					}
					break;
				}							
			}						
									
		}catch(Exception e){
			e.printStackTrace();
			this.closeStat();
		}
		
		return serverName;	
	}
	/**
	 * return all mapping between regions and the region server 
	 * @return
	 */
	public HashMap<HRegionInfo,String> getAllRegionAndRS(String tableName){
		List<HRegionInfo> regionList = this.getRegionInfoByTable(tableName);
		HashMap<HRegionInfo,String> regionInfoAndRS = new HashMap<HRegionInfo,String>();
		try{
			
			HashMap<String,String> regionAndRS = new HashMap<String,String>();
			ResultScanner result = hbaseUtil.getResultSet(null,null,null,null,1);
			for(Result r: result){				
				String rowKey = Bytes.toString(r.getRow());								
					List<KeyValue> pairs = r.list();				
					for(KeyValue kv:pairs){							
						if(Bytes.toString(kv.getQualifier()).equals("server")){							
							regionAndRS.put(rowKey, Bytes.toString(kv.getValue()));
							break;
						}
					}														
			}						
			
			for(int i=0;i<regionList.size();i++){
				HRegionInfo region = regionList.get(i);
				if(regionAndRS.containsKey(region.getRegionNameAsString())){
					regionInfoAndRS.put(region, regionAndRS.get(region.getRegionNameAsString()));					
				}				
			}						
		}catch(Exception e){
			e.printStackTrace();
			this.closeStat();
		}
		
		return regionInfoAndRS;	
	}
	
	public int getNumOfRegion(String tableName){
		List<HRegionInfo> regionList = this.getRegionInfoByTable(tableName);
		if(regionList != null)
			return regionList.size();
		return -1;
	}
	
	public List<String> getRegionNameAsList(String tableName){
		List<HRegionInfo> regionList = this.getRegionInfoByTable(tableName);
		List<String> names = new ArrayList<String>();
		for(int i=0;i<regionList.size();i++){
			names.add(regionList.get(i).getRegionNameAsString());
		}
		return names;
	}
	
	
	
	
}
