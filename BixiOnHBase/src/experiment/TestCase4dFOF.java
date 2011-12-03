package experiment;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.BixiQuerydFOFCluster;
import junit.framework.TestCase;

/*
 * This is for a sample , we need more test cases
 * 
 */
public class TestCase4dFOF extends TestCaseBase{

	@Override
	BixiQueryAbstraction getBixiQuery(){
		return new BixiQuerydFOFCluster();
	}
	
	public static void main(String[] args){
		TestCase4dFOF tests = new TestCase4dFOF();
		tests.runTests();
	}

}
