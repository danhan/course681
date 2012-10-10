package bixi.hbase.query.location;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import util.quadtree.based.trie.XQuadTree;
import bixi.dataset.collection.BixiReader;
import bixi.dataset.collection.XStation;
import bixi.hbase.query.BixiConstant;
import bixi.hbase.query.QueryAbstraction;
import bixi.query.coprocessor.BixiProtocol;

public class BixiLocationQueryS1S1 extends BixiLocationQueryS1{
	
	public BixiLocationQueryS1S1(){
		this.min_size_of_subspace = 1;
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+".S1";
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.STAT_FILE_NAME = "BixiLocationQueryS1S1.stat";
	}
	
	@Override
	public List<String> copQueryAvailableNear(String timestamp, final double latitude,
			final double longitude, final double radius) {
		this.getStatLog(STAT_FILE_NAME);
		
		long s_time = System.currentTimeMillis();
		
		try{			
		    /**Step1** Call back class definition **/
		    class BixiCallBack implements Batch.Callback< List<String> > {
		    	List<String>  res = new ArrayList<String> ();
		    	int count = 0;
		    	QueryAbstraction query = null;
		    	
		     public BixiCallBack(QueryAbstraction query){
		    	this.query = query; 
		     }
		      @Override
		      public void update(byte[] region, byte[] row,  List<String> result) {
		    	  count++;
		    	  //System.out.println((count)+": come back region: "+Bytes.toString(region)+"; result: "+result.size());
		    	  String outStr="count=>"+count+";region=>"+Bytes.toString(region)+";result=>"+result.size();
		    	  this.query.writeStat(outStr);
		    	  res.addAll(result); // to verify the error when large data
		      }		      
		    }		    
		    BixiCallBack callBack = new BixiCallBack(this);
		    
		    /**Step2*** generate scan***/ 
			// build up a quadtree.
			XQuadTree quadTree = new XQuadTree(space, min_size_of_subspace);
			quadTree.buildTree();

			double x = latitude - radius;
			double y = longitude - radius;								   
			// match rect to find the subspace it belongs to
			
			long match_s = System.currentTimeMillis();
			List<String> indexes = quadTree.match(x,y,2*radius,2*radius);
			long match_time = System.currentTimeMillis() - match_s;			
			
			// prepare filter for scan
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			for(String s:indexes){				
				if(s!=null){
					Filter rowFilter = hbaseUtil.getPrefixFilter(s);	
					fList.addFilter(rowFilter);	
				}				
			}
	    	Object[] objs = indexes.toArray();
	    	Arrays.sort(objs);
	    	String[] rowRanges= new String[2];
	    	rowRanges[0] = (String)objs[0];
	    	rowRanges[1] = (String)objs[objs.length-1]+"-*";
				    	
		    final Scan scan = hbaseUtil.generateScan(rowRanges,fList, null,null,-1);		    
		    
		    System.out.println("start to send the query to coprocessor.....");		    
		    
		    /**Step3: send request to trigger Coprocessor execution**/
		    hbaseUtil.getHTable().coprocessorExec(BixiProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<BixiProtocol,  List<String> >() {
		      
		    	public  List<String> call(BixiProtocol instance)
		          throws IOException {
		    		
		        return null; //instance.copQueryNeighbor4LS1(scan,latitude,longitude,radius); 			        
		        
		      };
		    }, callBack);
		    		    
		    
			long exe_time = System.currentTimeMillis()- s_time;
			// TODO store the time into database
			
			//System.out.println("exe_time=>"+exe_time+";result=>"+callBack.res.size());		
			String outStr = "m=>cop;"+"radius=>"+radius+";exe_time=>"+exe_time+";result=>"+callBack.res.size()+";match=>"+(match_time)+";subspace=>"+this.min_size_of_subspace;;
			this.writeStat(outStr);
			
		    return callBack.res;
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbaseUtil.closeTableHandler();
			this.closeStatLog();
		}
		
		return null;		
		
	}

	@Override
	public HashMap<String,String> scanQueryAvailableNear(String timestamp, double latitude,
			double longitude, double radius) {
		this.getStatLog(STAT_FILE_NAME);
		long sTime = System.currentTimeMillis();
		
		// build up a quadtree.
		XQuadTree quadTree = new XQuadTree(space, min_size_of_subspace);
		quadTree.buildTree();
		longitude = Math.abs(longitude);
		double x = latitude - radius;
		double y = longitude - radius;
		Point2D.Double point = new Point2D.Double(latitude,longitude);
		ResultScanner rScanner = null;
		//result container
		HashMap<String,String> results = new HashMap<String,String>();
		try{
			// match rect to find the subspace it belongs to
			long match_s = System.currentTimeMillis();
			List<String> indexes = quadTree.match(x,y,2*radius,2*radius);
			long match_time = System.currentTimeMillis() - match_s;
			// prepare filter for scan
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			for(String s:indexes){			
				if(s!=null){
					Filter rowFilter = hbaseUtil.getPrefixFilter(s);	
					fList.addFilter(rowFilter);	
				}				
			}	
	    	Object[] objs = indexes.toArray();
	    	Arrays.sort(objs);
	    	String[] rowRanges= new String[2];
	    	rowRanges[0] = (String)objs[0];
	    	rowRanges[1] = (String)objs[objs.length-1]+"-*";
			
			rScanner = this.hbaseUtil.getResultSet(rowRanges,fList, null,null,-1);
			BixiReader reader = new BixiReader();
			int count = 0;
			int accepted = 0;
			for(Result r: rScanner){
				//System.out.println(Bytes.toString(r.getRow()) + "=>");
				List<KeyValue> pairs = r.list();
				for(KeyValue kv:pairs){
					//System.out.println(Bytes.toString(kv.getRow())+"=>"+Bytes.toString(kv.getValue()));
					count++;
					// get the distance between this point and the given point
					XStation station = reader.getStationFromJson(Bytes.toString(kv.getValue()));
					station.setId(Bytes.toString(kv.getQualifier()));
					
					Point2D.Double resPoint = new Point2D.Double(station.getLatitude(),Math.abs(station.getlongitude()));
					double distance = resPoint.distance(point);
					
					if(distance <= radius){						
						//System.out.println("row=>"+Bytes.toString(r.getRow()) + ";colum=>"+Bytes.toString(kv.getQualifier())+ ";station=>"+station.getId());
						results.put(station.getId(), String.valueOf(distance));
						accepted++;
					}
						
				}
			}
			long eTime = System.currentTimeMillis();			
			//System.out.println("count=>"+count+";accepted=>"+accepted + ";time=>"+(eTime-sTime));
			String outStr = "m=>scan;"+"radius=>"+radius+";count=>"+count+";accepted=>"+accepted + ";time=>"+(eTime-sTime)+";match=>"+match_time+";subspace=>"+this.min_size_of_subspace;;
			this.writeStat(outStr);
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			this.hbaseUtil.closeTableHandler();
			this.closeStatLog();
		}
		return results;
	}
	
	
	

}
