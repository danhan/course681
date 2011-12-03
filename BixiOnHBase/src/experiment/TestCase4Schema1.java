package experiment;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import bixi.hbase.query.BixiQueryAbstraction;
import bixi.hbase.query.BixiQuerySchema1;

public class TestCase4Schema1 extends TestCaseBase{

	BixiQueryAbstraction getBixiQuery(){
		return new BixiQuerySchema1();
	}
	
	//convert to 01_10_2010__00 form
	protected String convertDate(String a){
		try{
			DateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
			Date date = formatter.parse(a);
			DateFormat newformat = new SimpleDateFormat("dd_MM_yyyy__HH");
			return newformat.format(date);
		}catch(Exception e){
			
		}
		return "";
	}
	
	public static void main(String[] args){		
		String timestamp = "2011100111";
		if (timestamp.length()<12) timestamp = timestamp+"00";
		System.out.println(timestamp);
		//TestCase4Schema1 tests = new TestCase4Schema1();
		//tests.runTests();
	}

}
