package experiment;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.BixiQueryQuadTreeCluster;

/*
 * This is for a sample , we need more test cases
 * 
 */
public class TestCase4dFOF extends TestCaseBase{

	@Override
	BixiQueryAbstraction getBixiQuery(){
		return new BixiQueryQuadTreeCluster(3);
	}
	
	public void queryAvailableByClusters(){
		System.out.println("******************queryAvailableByClusters**********************************");		
		String timestamp = tests.getProperty(singleTimeStamp);
		timestamp = convertDate(timestamp);		
		System.out.println("******************Start at ******"+timestamp+"****************************");
		
		new BixiQueryQuadTreeCluster(3).queryAvailableByClusters(timestamp);	
		System.out.println("******************End ******"+timestamp+"****************************");
	}	
	
	public static void main(String[] args){
		TestCase4dFOF tests = new TestCase4dFOF();
		tests.runTests();
		tests.queryAvailableByClusters();
		
	}

}
