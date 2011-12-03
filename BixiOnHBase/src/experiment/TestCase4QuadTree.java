package experiment;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.BixiQueryQuadTreeCluster;

public class TestCase4QuadTree extends TestCaseBase{
	
	
	@Override
	BixiQueryAbstraction getBixiQuery(){		
		return new BixiQueryQuadTreeCluster(2);
	}	
	
	public void queryAvailableByClusters(){
		System.out.println("******************queryAvailableByClusters**********************************");		
		String timestamp = tests.getProperty(singleTimeStamp);
		timestamp = convertDate(timestamp);		
		System.out.println("******************Start at ******"+timestamp+"****************************");
		
		new BixiQueryQuadTreeCluster(2).queryAvailableByClusters(timestamp);	
		System.out.println("******************End ******"+timestamp+"****************************");
	}
	
	public static void main(String[] args){
		TestCase4QuadTree tests = new TestCase4QuadTree();
		tests.runTests();
		tests.queryAvailableByClusters();
	}
	
	
	
}
