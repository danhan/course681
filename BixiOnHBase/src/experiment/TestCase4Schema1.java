package experiment;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.BixiQuerySchema1;

public class TestCase4Schema1 extends TestCaseBase{

	BixiQueryAbstraction getBixiQuery(){
		return new BixiQuerySchema1(1);
	}
	
	//convert to 01_10_2010__00 form
	protected String convertDate(String a){
		try{
			DateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
			Date date = formatter.parse(a);
			DateFormat newformat = new SimpleDateFormat("dd_MM_yyyy__HH");
			return newformat.format(date);
		}catch(Exception e){
			e.printStackTrace();
		}
		return "";
	}
	
	public static void main(String[] args){		
		TestCase4Schema1 tests = new TestCase4Schema1();
		if(args.length < 1){
			System.out.println("0: runTests; \n 1: runScanByBatch propertyname\n" +
		            "2: runScan4PointByBatch propertyname \n" +
		            "3: runCoprocessor4StationByBatch propertyname \n" +
		            "4: runCoprocessor4PointByBatch propertyname\n");
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
			
	}

}
