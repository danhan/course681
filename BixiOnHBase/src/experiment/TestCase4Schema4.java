package experiment;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.BixiQuerySchema3;
import bixi.hbase.query.BixiQuerySchema4;

public class TestCase4Schema4 extends TestCaseBase{
	
	@Override
	BixiQueryAbstraction getBixiQuery(){		
		return new BixiQuerySchema4(3);
	}	
	
	
	public static void main(String[] args){
		TestCase4Schema4 tests = new TestCase4Schema4();
		if(args.length < 1){
			System.out.println("0: runTests; \n 1: for runScanByBatch propertyname \n" +
		            "2: runScan4PointByBatch propertyname \n" +
		            "3: runCoprocessor4StationByBatch propertyname \n" +
		            "4: runCoprocessor4PointByBatch propertyname \n");
		}else{
			if(args[0].equals("0")){
				tests.runTests();
			}else if(args[0].equals("1")){
				tests.runScanByBatch(args[1]); // args[1] is the property name from property file
			}else if(args[0].equals("2")){
				tests.runScan4PointByBatch(args[1]);
			}else if(args[0].equals("3")){
				tests.runCoprocessor4StationByBatch(args[1]);
			}else if(args[0].equals("4")){
				tests.runCoprocessor4PointByBatch(args[1]);
			}
		}
		
		//
	}
}
