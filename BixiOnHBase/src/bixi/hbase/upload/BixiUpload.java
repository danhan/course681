package bixi.hbase.upload;

import java.io.File;
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
			}else if(schema == 11){ // location schema1
				for(int f=0;f<10;f++){
					TableInsertLocationS1 inserter = new TableInsertLocationS1();
					File dir = new File(fileDir);
					int batchNum = 100;
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName,batchNum);	
					System.out.println("finish the file"+fileName);
				}
	
			}else if(schema == 12){ // location schema2
				for(int f=0;f<10;f++){
					TableInsertLocationS2 inserter = new TableInsertLocationS2();
					File dir = new File(fileDir);
					
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName);						
					System.out.println("finish the file"+fileName);
				}				
			}
	   
	  }
}
