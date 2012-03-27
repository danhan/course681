package bixi.hbase.upload;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

public class BixiUpload {
	
	  public static void main(String[] args) throws ParserConfigurationException, IOException {
			if(args.length<2){
				return;
			}
			int schema = Integer.parseInt(args[0]);
			String fileDir = args[1];
			int batchRow = 10000;
			if(args.length>=3)
				batchRow = Integer.valueOf(args[2]);
			
			if(schema ==3){
				TableInsertSchema3 inserter = new TableInsertSchema3();					
				inserter.insertXmlData(schema,batchRow,fileDir);				
			}else if(schema == 4){
				TableInsertSchema4 inserter = new TableInsertSchema4();	
				inserter.insertXmlData(schema,batchRow,fileDir);			
			}
	   
	  }
}
