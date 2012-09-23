package experiment;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.QueryAbstraction;
import bixi.hbase.query.location.BixiLocationQueryS22;

public class TestCase4LocationS22 extends TestCaseBase{
	
	@Override
	BixiQueryAbstraction getBixiQuery() {
		return null;
	}
	
	QueryAbstraction getBixiLocationQuery(){
		return new BixiLocationQueryS22();
	}
	
	public static void main(String[] args){
		
		TestCase4LocationS22 tests = new TestCase4LocationS22();
		
		if(args.length < 1){
			System.out.println("0: runTests; \n " +
					"1: runScanQueryAvailable propertyname\n" +
		            "2: runCopQueryAvailable propertyname \n"+
		            "3: runScanQueryPoint propertyname \n"+
		            "4: runCopQueryPoint propertyname \n"+
            		"5: runScanQueryKNN propertyname \n"+
            		"6: runCopQueryKNN propertyname \n");
		}else{
			if(args[0].equals("0")){
				tests.runTests();
			}else if(args[0].equals("1")){
				tests.runScanQueryAvailable(args[1]); // args[1] is the property name from property file
			}else if(args[0].equals("2")){
				tests.runCopQueryAvailable(args[1]); // args[1] is the property name from property file
			}else if(args[0].equals("3")){
				tests.runScanQueryPoint(args[1]); // args[1] is the property name from property file
			}else if(args[0].equals("4")){
				tests.runCopQueryPoint(args[1]); // args[1] is the property name from property file
			}else if(args[0].equals("5")){
				tests.runScanQueryKNN(args[1]); // args[1] is the property name from property file
			}else if(args[0].equals("6")){
				tests.runCopQueryKNN(args[1]); // args[1] is the property name from property file
			}
		}
	}
	

}
