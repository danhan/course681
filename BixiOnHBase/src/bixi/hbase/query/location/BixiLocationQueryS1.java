package bixi.hbase.query.location;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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

/**
 * This class is to process the location query based on Location Schema1
 * @author dan
 *
 */
public class BixiLocationQueryS1 extends QueryAbstraction{
	
	double min_size_of_subspace = BixiConstant.MIN_SIZE_OF_SUBSPACE;
	
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
		
		long s_time = System.currentTimeMillis();
		
		try{			
		    /**Step1** Call back class definition **/
		    class BixiCallBack implements Batch.Callback< List<String> > {
		    	List<String>  res = new ArrayList<String> ();
		    	int count = 0;

		      @Override
		      public void update(byte[] region, byte[] row,  List<String> result) {
		    	  System.out.println((count++)+": come back region: "+Bytes.toString(region)+"; result: "+result.size());
		    	  res.addAll(result); // to verify the error when large data
		      }
		      
		    }		    
		    BixiCallBack callBack = new BixiCallBack();
		    
		    /**Step2*** generate scan***/ 
			// build up a quadtree.
			XQuadTree quadTree = new XQuadTree(space, min_size_of_subspace);
			quadTree.buildTree();

			double x = latitude - radius;
			double y = longitude - radius;								   
			// match rect to find the subspace it belongs to
			String[] indexes = quadTree.match(x,y,radius,radius);	
			// prepare filter for scan
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			for(String s:indexes){
				System.out.println(s);
				if(s!=null){
					Filter rowFilter = hbaseUtil.getPrefixFilter(s);	
					fList.addFilter(rowFilter);	
				}				
			}
		
		    final Scan scan = hbaseUtil.generateScan(null,fList, null,null,-1);		    
		    
		    System.out.println("start to send the query to coprocessor.....");		    
		    
		    /**Step3: send request to trigger Coprocessor execution**/
		    hbaseUtil.getHTable().coprocessorExec(BixiProtocol.class, scan.getStartRow(),scan.getStopRow(),
		    		new Batch.Call<BixiProtocol,  List<String> >() {
		      
		    	public  List<String> call(BixiProtocol instance)
		          throws IOException {
		    		
		        return instance.copQueryNeighbor4LS1(scan,latitude,longitude,radius);			        
		        
		      };
		    }, callBack);
		    
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+callBack.res.size());			    	
			
		    return callBack.res;
		    
		}catch(Exception e){
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbaseUtil.closeTableHandler();
		}
		
		return null;		
		
	}

	@Override
	public HashMap<String,String> scanQueryAvailableNear(String timestamp, double latitude,
			double longitude, double radius) {
		
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
			String[] indexes = quadTree.match(x,y,radius,radius);	
			// prepare filter for scan
			FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			for(String s:indexes){
				System.out.println(s);
				if(s!=null){
					Filter rowFilter = hbaseUtil.getPrefixFilter(s);	
					fList.addFilter(rowFilter);	
				}				
			}	
			
			rScanner = this.hbaseUtil.getResultSet(null,fList, null,null,-1);
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
			System.out.println("count is : "+count+"; accepted is "+accepted + ";time:"+(eTime-sTime));
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return results;
	}
	

	@Override
	public void copQueryPoint(double latitude, double longitude) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void scanQueryPoint(double latitude, double longitude) {
		
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
			rScanner = this.hbaseUtil.getResultSet(new String[]{node.getIndex(),node.getIndex()},null, null,null,-1);
			int count = 0;
		
			for(Result r: rScanner){
				//System.out.println(Bytes.toString(r.getRow()) + "=>");
				List<KeyValue> pairs = r.list();
				for(KeyValue kv:pairs){
					//System.out.println(Bytes.toString(kv.getRow())+"=>"+Bytes.toString(kv.getValue()));
					XStation station = reader.getStationFromJson(Bytes.toString(kv.getValue()));
					//station.print();
					System.out.println(station.getLatitude()+"<>"+latitude);
					System.out.println(station.getlongitude()+"<>"+longitude);
					System.out.println(Bytes.toString(kv.getQualifier()));
					if((station.getLatitude() == latitude && station.getlongitude() == longitude)){
						
						System.out.println(Bytes.toString(kv.getQualifier()));
						//break;
					}
					count++;
				}
			}	
			long eTime = System.currentTimeMillis();
			System.out.println("count=>"+count + "; time=>"+(eTime-sTime));
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			this.hbaseUtil.closeTableHandler();
		}
		
	}

	@Override
	public void copQueryArea(double latitude, double longitude, int area) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void scanQueryArea(double latitude, double longitude, int area) {
		// TODO Auto-generated method stub
		
	}

	/****************************The following one would be verified later******************************/
	
	@Override
	public void copQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int n) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void scanQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int n) {
		// TODO Auto-generated method stub
	
	}	
	
	
}
