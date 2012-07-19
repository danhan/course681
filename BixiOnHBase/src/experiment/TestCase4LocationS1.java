package experiment;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.QueryAbstraction;
import bixi.hbase.query.location.BixiLocationQueryS1;

public class TestCase4LocationS1 extends TestCaseBase{

	@Override
	BixiQueryAbstraction getBixiQuery() {
		return null;
	}
	
	QueryAbstraction getBixiLocationQuery(){
		return new BixiLocationQueryS1();
	}
	
	public static void main(String[] args){
		
		TestCase4LocationS1 tests = new TestCase4LocationS1();
		
		if(args.length < 1){
			System.out.println("0: runTests; \n " +
					"1: runScanQueryAvailable propertyname\n" +
		            "2: runCopQueryAvailable propertyname \n");
		}else{
			if(args[0].equals("0")){
				tests.runTests();
			}else if(args[0].equals("1")){
				tests.runScanQueryAvailable(args[1]); // args[1] is the property name from property file
			}else if(args[0].equals("2")){
				tests.runCopQueryAvailable(args[1]); // args[1] is the property name from property file
			}
		}
	}
	

}
