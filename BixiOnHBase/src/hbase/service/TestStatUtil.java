package hbase.service;


public class TestStatUtil {

	
	public static void main(String[] args){
		StatUtil stat = new StatUtil();
		String table1 = "bixi.location.1";
		String table2 = "bixi.location.2";
		
		System.out.println(stat.getNumOfRegion("bixi.location.2"));
		System.out.println(stat.getNumOfRegion("bixi.location.1"));		
		System.out.println(stat.getRegionNameAsList(table1));
		System.out.println(stat.getRSbyRegion("bixi.location.1,,1349980508430.03de533e160bc97528620f88f30e73e7."));
		System.out.println(stat.getAllRegionAndRS(table1));
		
	}
}
