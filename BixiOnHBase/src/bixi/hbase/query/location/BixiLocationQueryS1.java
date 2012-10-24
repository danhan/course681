package bixi.hbase.query.location;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HRegionInfo;
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
import bixi.query.coprocessor.RCopResult;

/**
 * Subspace = 0.1
 * This class is to process the location query based on Location Schema1
 * Schema1 is to group locations with QuadTree
 * @author dan
 *
 */
public class BixiLocationQueryS1 extends QueryAbstraction{
	
	double min_size_of_subspace = BixiConstant.MIN_SIZE_OF_SUBSPACE0;
	String STAT_FILE_NAME = "SpaceS1-01.stat";
	String FILE_NAME_PREFIX = "SpaceS1-01";
	
	
	public BixiLocationQueryS1(){
		tableName = BixiConstant.LOCATION_TABLE_NAME_1;
		familyName = new String[]{BixiConstant.LOCATION_FAMILY_NAME};
		try{
			this.setHBase();
		}catch(Exception e){
			e.printStackTrace();
		}			
		
	}
		
	@Override
	public List<String> copQueryAvailableNear(String timestamp, final double latitude,
			final double longitude, final double radius) {
		this.getStatLog(STAT_FILE_NAME);
		this.getCSVLog(FILE_NAME_PREFIX,0);
		this.getCSVLog(FILE_NAME_PREFIX, 1);
		this.getCSVLog(FILE_NAME_PREFIX,2);
		
		this.timePhase.clear();
				
		try{			
		    /**Step1** Call back class definition **/

		    class BixiCallBack implements Batch.Callback< RCopResult> {
		    	RCopResult res = new RCopResult();
		    	int count = 0; // the number of coprocessor
		    	QueryAbstraction query = null;
		    	
		     public BixiCallBack(QueryAbstraction query){
		    	this.query = query; 
		     }
		      @Override
		      public void update(byte[] region, byte[] row,  RCopResult result) {
		    	  long current = System.currentTimeMillis();
		    	  count++;		    	 
		    	  res.getRes().addAll(result.getRes()); // to verify the error when large data
		    	  res.setStart(result.getStart());
		    	  res.setEnd(result.getEnd());
		    	  res.setRows((res.getRows()+result.getRows()));	
		    	  res.setCells(res.getCells()+result.getCells());
		    	  
/*		    	  String outStr="count=>"+count+";start=>"+result.getStart()+";end=>"+result.getEnd()+";process=>"+(result.getEnd()-result.getStart())+
		    			  ";transmission=>"+(System.currentTimeMillis()-result.getEnd())+
		    			  ";rs=>"+Bytes.toString(region)+
		    			  ";row=>"+result.getRows()+";result=>"+result.getRes().size();		    	  
		    	  this.query.writeStat(outStr);
		    	  outStr = query.regions.get(Bytes.toString(region)).toString();
		    	  this.query.writeStat(outStr);*/
		    	  // write them into csv file
		    	  String outStr = "";
		    	  outStr += "within,"+"cop,"+result.getParameter()+","+result.getStart()+","+
		    			  	result.getEnd()+","+current+","+
		    			  	result.getRows()+","+result.getCells()+","+result.getKvLength()+","+result.getRes().size()+","+
		    			  	this.query.regionAndRS.get(Bytes.toString(region))+","+Bytes.toString(region);
		    			  	//this.query.regionAndRS.get(Bytes.toString(region));
		    	  this.query.writeCSVLog(outStr,1);
		    	  		    	  
		      }		      
		    }
		    
		    BixiCallBack callBack = new BixiCallBack(this);
		    
		    /**Step2*** generate scan***/ 
			// build up a quadtree.
			long s_time = System.currentTimeMillis();
			this.timePhase.add(s_time);	
			
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
		    long cop_start = System.currentTimeMillis();		    
		    this.timePhase.add(cop_start);
		    
		    hbaseUtil.getHTable().coprocessorExec(BixiProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<BixiProtocol,  RCopResult >() {
		      
		    	public  RCopResult call(BixiProtocol instance)
		          throws IOException {
		    		
		        return instance.copQueryNeighbor4LS1(scan,latitude,longitude,radius);			        
		        
		      };
		    }, callBack);
		    
		    long cop_end = System.currentTimeMillis();
		    this.timePhase.add(cop_end);				   
		    
			long exe_time = cop_end- s_time; 	
/*			String outStr = "q=>within;m=>cop;"+"radius=>"+radius+";exe_time=>"+exe_time+";cop_start=>"+cop_start+
					";result=>"+callBack.res.getRes().size()+";match=>"+(match_time)+";subspace=>"+this.min_size_of_subspace;
			this.writeStat(outStr);*/
			// write to csv file
			String outStr = "";
			outStr += "within,"+"cop,"+callBack.res.getCells()+","+callBack.res.getRes().size()+","+exe_time+","
						+callBack.res.getRows()+","+match_time+","+this.min_size_of_subspace+","+radius;	
					
			this.writeCSVLog(outStr, 0);
						
			long tail = 0;
			outStr = "";
			String timeStr = "within,cop,"+radius+",";
			for(int i=0;i<this.timePhase.size();i++){				
				outStr += (this.timePhase.get(i)-tail) + ";";				
				tail = this.timePhase.get(i);
				timeStr +=this.timePhase.get(i)+",";
			}
			outStr += (tail-this.timePhase.get(0))+"\n";
			//this.writeStat(outStr);
			// write it to csv file
			this.writeCSVLog(timeStr,2);
			
		    return callBack.res.getRes();
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbaseUtil.closeTableHandler();			
			this.closeStatLog();
			this.closeCSVLog();			
		}
		
		return null;		
		
	}

	@Override
	public HashMap<String,String> scanQueryAvailableNear(String timestamp, double latitude,
			double longitude, double radius) {
		this.getStatLog(STAT_FILE_NAME);
		long sTime = System.currentTimeMillis();
		this.getCSVLog(FILE_NAME_PREFIX,0);		
		this.getCSVLog(FILE_NAME_PREFIX,2);
		this.timePhase.clear();
		
		this.timePhase.add(System.currentTimeMillis());
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
			
	    	this.timePhase.add(System.currentTimeMillis());
			rScanner = this.hbaseUtil.getResultSet(rowRanges,fList, null,null,-1);
			BixiReader reader = new BixiReader();
			int count = 0;
			int accepted = 0;
			int row = 0;
			for(Result r: rScanner){
				//System.out.println(Bytes.toString(r.getRow()) + "=>");
				List<KeyValue> pairs = r.list();
				row++;
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
			this.timePhase.add(eTime);
			//System.out.println("count=>"+count+";accepted=>"+accepted + ";time=>"+(eTime-sTime));
			String outStr = "m=>scan;"+"radius=>"+radius+";count=>"+count+";accepted=>"+accepted + ";time=>"+(eTime-sTime)+";row=>"+row+";match=>"+match_time+";subspace=>"+this.min_size_of_subspace;;
			this.writeStat(outStr);
			
			// write to csv file
			outStr = "";
			outStr += "within,"+"scan,"+count+","+accepted+","+(eTime-sTime)+","+row+
					  ","+match_time+","+this.min_size_of_subspace+","+radius;	
					
			this.writeCSVLog(outStr, 0);
			
			String timeStr = "within,scan,"+radius+",";			
			for(int i=0;i<this.timePhase.size();i++){
				timeStr += this.timePhase.get(i)+",";
			}
			this.writeCSVLog(timeStr, 2);
			
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			this.hbaseUtil.closeTableHandler();
			this.closeStatLog();
			this.closeCSVLog();
		}
		return results;
	}
	

	@Override
	public String copQueryPoint(final double latitude, final double longitude) {
		
		this.getStatLog(STAT_FILE_NAME);
		
		long s_time = System.currentTimeMillis();
		
		try{			
		    /**Step1** Call back class definition **/
		    class BixiCallBack implements Batch.Callback< String> {
		    	String  res = null;
		    	int count = 0;
		    	QueryAbstraction query = null;
		    	
		     public BixiCallBack(QueryAbstraction query){
		    	this.query = query; 
		     }
		      @Override
		      public void update(byte[] region, byte[] row,  String result) {
		    	  count++;
		    	  
		    	  String outStr="endTime="+System.currentTimeMillis()+";region=>"+Bytes.toString(region)+
		    			  ";count=>"+count+";result=>"+result;		    	  
		    	  this.query.writeStat(outStr);	
		    	  outStr = query.regions.get(Bytes.toString(region)).toString();
		    	  this.query.writeStat(outStr);
		    	  res = result;
		      }		      
		    }		    
		    BixiCallBack callBack = new BixiCallBack(this);
		    
		    /**Step2*** generate scan***/ 
			// build up a quadtree.
			XQuadTree quadTree = new XQuadTree(space, min_size_of_subspace);
			quadTree.buildTree();								   
			// match rect to find the subspace it belongs to
			
			long match_s = System.currentTimeMillis();
			XQuadTree node = quadTree.locate(latitude, longitude);
			long match_time = System.currentTimeMillis() - match_s;			
			
	    	String[] rowRanges= new String[2];
	    	rowRanges[0] = node.getIndex();
	    	rowRanges[1] = node.getIndex()+"-*";
				    	
		    final Scan scan = hbaseUtil.generateScan(rowRanges,null, null,null,-1);		    		    		   
		    
		    System.out.println("start to send the query to coprocessor.....");		    
		    
		    /**Step3: send request to trigger Coprocessor execution**/
		    hbaseUtil.getHTable().coprocessorExec(BixiProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<BixiProtocol,  String >() {
		      
		    	public  String call(BixiProtocol instance)
		          throws IOException {
		    		
		        return instance.copQueryPoint4LS1(scan, latitude, longitude);			        
		        
		      };
		    }, callBack);
		    		    		    
			long exe_time = System.currentTimeMillis()- s_time;			
			
			
			//System.out.println("exe_time=>"+exe_time+";result=>"+callBack.res.size());		
			String outStr = "q=point;m=>cop;"+";exe_time=>"+exe_time+";result=>"+callBack.res+";match=>"+(match_time)+";subspace=>"+this.min_size_of_subspace;;
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
	public String scanQueryPoint(double latitude, double longitude) {
		
		this.getStatLog(STAT_FILE_NAME);
		
		long sTime = System.currentTimeMillis();
		// build up a quadtree.
		XQuadTree quadTree = new XQuadTree(space, min_size_of_subspace);
		quadTree.buildTree();		
		ResultScanner rScanner = null;

		try{
			// match rect to find the subspace it belongs to
			XQuadTree node = quadTree.locate(latitude, longitude);
			System.out.println(node.getIndex());
			BixiReader reader = new BixiReader();
			rScanner = this.hbaseUtil.getResultSet(new String[]{node.getIndex(),node.getIndex()+"0"},null, null,null,-1);
			int count = 0;
			int row = 0;
			String stationName = null;
			for(Result r: rScanner){
				//System.out.println(Bytes.toString(r.getRow()) + "=>");
				List<KeyValue> pairs = r.list();
				row++;
				for(KeyValue kv:pairs){
					//System.out.println(Bytes.toString(kv.getRow())+"=>"+Bytes.toString(kv.getValue()));
					XStation station = reader.getStationFromJson(Bytes.toString(kv.getValue()));
					//station.print();
					//System.out.println(station.getLatitude()+"<>"+latitude);
					//System.out.println(station.getlongitude()+"<>"+longitude);
					//System.out.println(Bytes.toString(kv.getQualifier()));
					if((station.getLatitude() == latitude && station.getlongitude() == longitude)){
						stationName = Bytes.toString(kv.getQualifier());
						System.out.println(Bytes.toString(kv.getQualifier()));
						break;
					}
					count++;
				}
				if(stationName != null)
					break;
			}	
			long eTime = System.currentTimeMillis();			
			String outStr = "q=point;m=scan;"+"count=>"+count + "; time=>"+(eTime-sTime)+";row=>"+row+";result=>"+stationName;
			this.writeStat(outStr);
			return stationName;
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			this.hbaseUtil.closeTableHandler();
			this.closeStatLog();
		}
		return null;
		
	}

	@Override
	public List<Point2D.Double> debugColumnVersion(String timestamp,
			double latitude, double longitude, double radius){
		this.getStatLog(this.STAT_FILE_NAME);
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
		List<Point2D.Double> returnedPoints = new ArrayList<Point2D.Double>();
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
			/****/
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
						returnedPoints.add(resPoint);
						//System.out.println("row=>"+Bytes.toString(r.getRow()) + ";colum=>"+Bytes.toString(kv.getQualifier())+ ";station=>"+station.getId());
						results.put(station.getId(), String.valueOf(distance));						
						accepted++;
					}
						
				}
			}					
			long eTime = System.currentTimeMillis();
			System.out.println("count=>"+count+";accepted=>"+accepted + ";time=>"+(eTime-sTime));
			String outStr = "count=>"+count+";accepted=>"+accepted + ";time=>"+(eTime-sTime)+";match=>"+(match_time)+";subspace=>"+this.min_size_of_subspace;
			this.writeStat(outStr);						
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			this.hbaseUtil.closeTableHandler();
			this.closeStatLog();
		}
		
		return returnedPoints;
	}
	
	
	/**
	 * Query the area north/south/west/east of the given point
	 */
	@Override
	public void copQueryArea(double latitude, double longitude, int area) {
		// TODO Auto-generated method stub
		
	}
	/**
	 * Query the area north/south/west/east of the given point
	 */
	@Override
	public void scanQueryArea(double latitude, double longitude, int area) {
		// TODO Auto-generated method stub
		
	}

	/****************************The following one would be verified later******************************/
	
	@Override
	public void copQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int k) {
		// TODO Auto-generated method stub
		
	}
	/**
	 * Using density-based estimation method
	 */
	@Override
	public TreeMap<Double,String> scanQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int k) {
		this.getStatLog(STAT_FILE_NAME);
		long sTime = System.currentTimeMillis();
		// Step1: estimate the window circle for the first time
		int total_points = Integer.valueOf(this.conf.getProperty("total_num_of_points")); //1000000000;//
		double areaOfMBB = this.space.width * this.space.height;
		double DensityOfMBB = total_points / areaOfMBB;
		double init_radius = Math.sqrt(k / DensityOfMBB);
		
		XQuadTree quadTree = new XQuadTree(space, min_size_of_subspace);
		quadTree.buildTree();
		longitude = Math.abs(longitude);
		double x = latitude - init_radius;
		double y = longitude - init_radius;
		int count = 0;
		int accepted = 0;
		ResultScanner rScanner = null;
		HashMap<String,String> results = new HashMap<String,String>();
		BixiReader reader = new BixiReader();
		TreeMap<Double,String> sorted = null;
		try{	
			// Step2: trigger a scan to get the points based on the above window		
			int iteration = 1;
			double radius = (init_radius > this.min_size_of_subspace)? init_radius:this.min_size_of_subspace;
			long match_s = System.currentTimeMillis();
			do{
				String str = "iteration"+ iteration+"; count=>"+count+";radius=>"+radius;
				this.writeStat(str);
				
				List<String> indexes = quadTree.match(x,y,2*radius,2*radius);
				
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
				count = 0;
				results.clear();
				for(Result r: rScanner){
					//System.out.println(Bytes.toString(r.getRow()) + "=>");
					List<KeyValue> pairs = r.list();
					for(KeyValue kv:pairs){
						//System.out.println(Bytes.toString(kv.getRow())+"=>"+Bytes.toString(kv.getValue()));
						results.put(Bytes.toString(kv.getQualifier()),Bytes.toString(kv.getValue()));
						count++;						
					}
				}
				radius = init_radius * (iteration+1);
				
				// Step3: get the result,estimate the window circle next depending on the previous step result, util we got the K nodes
/*				radius = (radius*(iteration+1) > this.min_size_of_subspace*(iteration+1))? 
						radius*(iteration+1): this.min_size_of_subspace*(iteration+1);*/
/*				if(count == 0 && iteration ==1){ // when the first time count == 0
					radius = radius * 2;
				}else if(count > 0 && iteration >0){ // when the first time count >0 && count < k
					areaOfMBB = radius * radius;
					DensityOfMBB = count / areaOfMBB;
					radius = Math.sqrt(k / DensityOfMBB);	
					
				}	*/		
				
			}while(count < k && (++iteration>0));
			
			String str = "iteration"+ iteration+"; count=>"+count+";radius=>"+radius;
			this.writeStat(str);
			
			long match_time = System.currentTimeMillis() - match_s;
			
			// Step4: get all possible points and sort them by the distance and get the top K
			Point2D.Double point = new Point2D.Double(latitude,longitude);
			//result container
			HashMap<Double,String> distanceMap = new HashMap<Double,String>();			
			for(String key:results.keySet()){				
				XStation station = reader.getStationFromJson(results.get(key));
				station.setId(key);
				
				Point2D.Double resPoint = new Point2D.Double(station.getLatitude(),Math.abs(station.getlongitude()));
				double distance = resPoint.distance(point);
				distanceMap.put(distance,key);											
			}			
			
			sorted = new TreeMap<Double,String>(distanceMap);
			
			long eTime = System.currentTimeMillis();						
			String outStr = "q=knn;m=>scan;"+";count=>"+count+";accepted=>"+accepted + ";time=>"+(eTime-sTime)+";k=>"+k;
			this.writeStat(outStr);
			
		}catch(Exception e){
				e.printStackTrace();
		}finally{
				this.hbaseUtil.closeTableHandler();
				this.closeStatLog();
		}
	
			return sorted;
	}	
		
}
