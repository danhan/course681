package experiment;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.BixiQueryQuadTreeCluster;
import bixi.hbase.query.BixiQuerydFOFCluster;
import junit.framework.TestCase;

public class TestCase4QuadTree extends TestCaseBase{
	
	@Override
	BixiQueryAbstraction getBixiQuery(){
		return new BixiQueryQuadTreeCluster();
	}	
	
	public static void main(String[] args){
		TestCase4QuadTree tests = new TestCase4QuadTree();
		tests.runTests();
	}
	
}
