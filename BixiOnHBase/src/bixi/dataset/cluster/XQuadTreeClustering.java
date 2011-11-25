package bixi.dataset.cluster;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import bixi.dataset.collection.BixiReader;
import bixi.dataset.collection.XLocation;
import bixi.dataset.collection.XQuadNode;
import bixi.dataset.collection.XQuadSpace;
import bixi.dataset.collection.XStation;

public class XQuadTreeClustering {
	
	private static int MAX_DEPTH = 3;
		
	BixiReader reader = null;
	private XQuadSpace space = null;
	private List<XStation> stations = null;
	private XClusterTable table = null;
	
	public XQuadTreeClustering(){		
		reader = new BixiReader();	
		table = new XClusterTable();
	}
		
	/*
	 * filename : full path for file
	 */
	public void doClustering(String filename){
		try{
			stations = reader.parseXML(filename);

			// get left top point, distance dx, dy 
			this.space = getQuadSpace();
			
			getClusterIds();		
			
		}catch(Exception e){
			e.printStackTrace();
		}	
		
	}
	
	private XQuadSpace getQuadSpace(){
		
		double x_min = Double.MAX_VALUE,x_max = Double.MIN_VALUE;
		double y_min = Double.MIN_VALUE,y_max = Double.MAX_VALUE;
				
		for(int i=0;i<stations.size();i++){			
			XLocation location = stations.get(i).getPoint();			
			double x = location.getLatitude();
			double y = location.getLongitude();
			if ( x_min > x ) x_min = x;			
			if (x_max < x) x_max = x;
			
			//System.out.println("min: "+y_min + ",max: "+ y_max + ", y: "+y);
			if(Math.abs(y_min) < Math.abs(y)) y_min = y;
			if(Math.abs(y_max) > Math.abs(y)) y_max = y;			
			//System.out.println("min: "+y_min + ",max: "+ y_max + ", y: "+y);
		}
	
		double dx = x_max - x_min;
		double dy = Math.abs(y_max-y_min);
//		System.out.println(dx+","+dy);
//		DecimalFormat df = new DecimalFormat("000.00000000");
//		dx = Double.valueOf(df.format(dx));
//		dy = Double.valueOf(df.format(dy));
		space = new XQuadSpace(new XLocation(x_min,y_max),dx,dy,MAX_DEPTH);
		space.print_space();
		
		return space;
	}
	
	private void getClusterIds(){
			
		for(int i=0;i<stations.size();i++){
			XStation station = stations.get(i);
			XLocation location = station.getPoint();
			XQuadNode node = space.locate(location);		
			if(node != null){
				station.setClusterId(node.getId());
			}else{
				System.out.println("failed to find its cluster id");
				location.print_location();
			}
		}
	
	}
		
	public Hashtable<String,List<String>> getClusters(){
		return this.table.getClusters();
	}	
	
	
	public List<XStation> getStations() {
		return stations;
	}

	public XStation getOneStation(String station_id){
		XStation station = null;
		for(int i=0;i<this.stations.size();i++){
			if(station_id.equals(stations.get(i).getId())){
				station = stations.get(i);
				return station;
			}
		}
		return station;
	}
	
	private void getClusterIds_debug(){
		
		System.out.println("get cluster id ");
		XLocation location = new XLocation(45.514565,-73.690909);
		XQuadNode node = space.locate(location);
		if(node != null){
			location.print_location();
			node.print();
		}else{
			System.out.println("node is null");
		}
	
	}	
	
	
	public void aggreateCluster(){
			
		for(int i=0;i<stations.size();i++){
			table.addStation(stations.get(i).getClusterId(), stations.get(i).getId());
		}
		table.print();		
	}
	
	
	private class XClusterTable{		
		private Hashtable<String,List<String>> clusters = null;
		
		public XClusterTable(){
			this.clusters = new Hashtable<String,List<String>>();
		}
		public void addStation(String cluster_id,String station_id){			
			if(this.clusters.containsKey(cluster_id)){
				this.clusters.get(cluster_id).add(station_id);
			}else{
				this.clusters.put(cluster_id, new LinkedList<String>());
				this.clusters.get(cluster_id).add(station_id);
			}			
		}
		
		
		public void print(){
			
			Set<String> keys = this.clusters.keySet();
			Iterator<String> ie = keys.iterator();
			int counter = 0;
			while(ie.hasNext()){
				String cluster = ie.next();
				System.out.print(cluster+": ( ");	
				Iterator<String> ids = this.clusters.get(cluster).iterator();
				int index  = 0;
				while(ids.hasNext()){
					System.out.print(ids.next()+";");
					index++;
					counter++;
				}
				System.out.println(")=>" + index+"=="+counter);
			}					
		}
		public Hashtable<String, List<String>> getClusters() {
			return clusters;
		}
		
		
	}
	
	
	public static void main(String[] args){
		
		XQuadTreeClustering clustering = new XQuadTreeClustering();
		File dir = new File("data2");
		String filename = dir.getAbsolutePath()+"/01_10_2010__00_00_01.xml";
		clustering.doClustering(filename);
		clustering.aggreateCluster();
		Hashtable<String,List<String>> table = clustering.getClusters();
		
	}
	
	
	
	
//	private List<XLocation> getLocations(List<XStation> stations){
//		
//		List<XLocation> location_list = new LinkedList<XLocation>();
//		for(int i=0;i<stations.size();i++){
//			XLocation location = new XLocation(stations.get(i).getLatitude(),stations.get(i).getLongtitude());
//			location_list.add(location);
//		}
//		return location_list;		
//	}	
	
}
