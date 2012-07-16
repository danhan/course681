package bixi.hbase.query.location;

import java.util.HashMap;
import java.util.TreeMap;

import bixi.dataset.statistics.BixiStatistics;

public class TestLocationQuery {

	public static void main(String args[]){
		
		double x = 45.51038;
		double y = -73.55653;
		double radius = 0.02;
		
		BixiLocationQueryS2 query2=new BixiLocationQueryS2();
		BixiLocationQueryS1 query1=new BixiLocationQueryS1();
		
		int option = 1;
		if(option == 0){ // query neigbours
			
			for(int i=0;i<10;i++){
				query2.scanQueryAvailableNear("",x,y,radius);	
			}
			
			System.out.println("=========================");
			
			
			for(int i=0;i<10;i++){
				query1.scanQueryAvailableNear("",x,y,radius);	
			}
		}else if(option == 1){ // query point
			query1.scanQueryPoint(x,Math.abs(y));
			query2.scanQueryPoint(x,y);
			
		}else if(option ==2){
			
		}
		

		
			

		
		
/*		BixiStatistics stat = new BixiStatistics();		
		HashMap<String,String> stations = stat.QueryNeighbor(x, y, radius);*/
		
//		TreeMap<String,String> tree = new TreeMap<String,String>(stations);
//		TreeMap<String,String> result2Tree = new TreeMap<String,String>(result1);
//		
//		// compare the two results
//		System.out.println(stations.size()+" <> "+result1.size());
//		//tree.keySet().remove(result2.keySet());
//		System.out.println("deduction: "+tree.size()+"=>"+tree.toString());
//		
//		
//		//result2Tree.keySet().remove(tree.keySet());
//		System.out.println("deduction: "+result2Tree.size()+"=>"+result2Tree.toString());
//
//		System.out.println("=============");
//		System.out.println(tree.descendingKeySet().toString());
//		System.out.println(result2Tree.descendingKeySet().toString());
		
	}
}
