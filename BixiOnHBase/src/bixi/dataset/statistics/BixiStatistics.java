package bixi.dataset.statistics;

import java.awt.geom.Point2D;
import java.awt.geom.Point2D.Double;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import bixi.dataset.collection.BixiReader;
import bixi.dataset.collection.XStation;

public class BixiStatistics {

	BixiReader reader = null;
	
	
	public BixiStatistics(){
		reader = new BixiReader();
		try{
			reader.parseXML("./data2/sub/01_10_2010__00_00_01.xml");	
		}catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	public static void main(String args[]){
		double x = 45.51038;
		double y = -73.55653;
		double radius = 0.02;
		
		BixiStatistics stat = new BixiStatistics();		
		stat.QueryNeighbor(x, y, radius);
	}
	
	/**
	 * 
	 * @param x
	 * @param y
	 * @param radius
	 */
	public HashMap<String,String> QueryNeighbor(double x, double y, double radius){
		
		Point2D.Double point = new Point2D.Double(x,Math.abs(y));
		HashMap<String,String> result = new HashMap<String,String>();
		for(XStation s:reader.stationList){
			Point2D.Double resPoint = new Point2D.Double(s.getLatitude(),Math.abs(s.getlongitude()));
			double distance = point.distance(resPoint);
			if(distance <= radius){
				result.put(s.getId(),String.valueOf(distance));
			}
			
		}
		System.out.println(x+";"+y+"(+-)"+radius+" neighbor:"+result.size());
		System.out.println(result.toString());
		System.out.println(result.keySet().toString());
		return result;
	}
	
}
