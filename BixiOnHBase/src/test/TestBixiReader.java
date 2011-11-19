package test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import bixi.dataset.collection.BixiReader;
import bixi.dataset.collection.XStation;
import junit.framework.TestCase;

public class TestBixiReader extends TestCase{
	
	BixiReader reader = new BixiReader();
	
	public void test_parseXML() throws IOException{
	    String fileDir = "data2";
	    File dir = new File(fileDir);
	    
		List<XStation> list = reader.parseXML(dir.getAbsolutePath()
							+"/"+"01_10_2010__00_00_01.xml");
		for(int i=0; i<list.size();i++)
			list.get(i).print();
		
	}
	

}
