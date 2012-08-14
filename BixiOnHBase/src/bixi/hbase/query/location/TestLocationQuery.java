package bixi.hbase.query.location;

import java.awt.geom.Point2D;
import java.awt.geom.Point2D.Double;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import bixi.dataset.statistics.BixiStatistics;

public class TestLocationQuery {

	public static void main(String args[]){
		
		// 1.2064190766706284;longitude=1.1019155104863243
		
		double x = 1.111;//70.51038;
		double y = 1.111;//3.55653;
		double radius = 0.11;//100;//4
		
		BixiLocationQueryS2 query2=new BixiLocationQueryS2();	
		BixiLocationQueryS21 query21=new BixiLocationQueryS21();
		//BixiLocationQueryS22 query22=new BixiLocationQueryS22();
		
		BixiLocationQueryS1 query1=new BixiLocationQueryS1();
		BixiLocationQueryS1 query11=new BixiLocationQueryS11();
		
		int runTime = 1;
		
		int option = 3;
		if(option == 0){ // query neigbours
			
			for(int i=0;i<runTime;i++){
				query2.scanQueryAvailableNear("",x,y,radius);				
				//query2.copQueryAvailableNear("",x, y,radius);
			}
			System.out.println("=========================");
						
			for(int i=0;i<runTime;i++){
				//query1.scanQueryAvailableNear("",x,y,radius);	
				//query1.copQueryAvailableNear("",x,y,radius);				
			}
		}else if(option == 1){ // query point
			for(int i=0;i<runTime;i++)
				query1.scanQueryPoint(x,y);
			System.out.println("=========================");
			for(int i=0;i<runTime;i++)
				query2.scanQueryPoint(x,y);
			
		}else if(option ==2){ // debug for schema2 for different lenght of row, column and version
			List<Point2D.Double> s2Point = null;
			List<Point2D.Double> s21Point = null;			
			for(int i=0;i<runTime;i++){
				s2Point = query2.debugColumnVersion("",x,y,radius);				
				System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
				s21Point = query21.debugColumnVersion("",x,y,radius);	
				//query22.debugColumnVersion("",x,y,radius);	
			}
			
		}else if(option ==3){ // debug for schema2 for different lenght of row, column and version
			List<Point2D.Double> s1Point = null;
			List<Point2D.Double> s11Point = null;	
			List<Point2D.Double> temp = new ArrayList<Point2D.Double>();
			for(int i=0;i<runTime;i++){
				s1Point = query1.debugColumnVersion("",x,y,radius);	
				temp.addAll(s1Point);
				System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
				s11Point = query11.debugColumnVersion("",x,y,radius);
				
				s1Point.removeAll(s11Point);
				System.out.println("in S1 not in S11 : "+s1Point.size()+"=>"+s1Point.toString());
				
				s11Point.removeAll(temp);
				System.out.println("in S11 not in S1 : "+s11Point.size()+"=>"+s11Point.toString());
				
				query2.debugColumnVersion("",x,y,radius);
				System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
				query21.debugColumnVersion("",x,y,radius);
				System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
			}
			
		}
		

		
			

		
		
		//BixiStatistics stat = new BixiStatistics();		
		//HashMap<String,String> stations = stat.QueryNeighbor(x, y, radius);
		
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
