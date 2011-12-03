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
		tests.runTests();
	}

}
