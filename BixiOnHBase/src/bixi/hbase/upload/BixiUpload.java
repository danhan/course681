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
			int fileNum = 10;
			if(args.length>=3)
				fileNum = Integer.parseInt(args[2]);
			
			int batchRow = 10000;
			if(args.length>=4)
				batchRow = Integer.valueOf(args[3]);			
			
			if(schema ==3){
				TableInsertSchema3 inserter = new TableInsertSchema3();					
				inserter.insertXmlData(schema,batchRow,fileDir);				
			}else if(schema == 4){
				TableInsertSchema4 inserter = new TableInsertSchema4();	
				inserter.insertXmlData(schema,batchRow,fileDir);			
			}else if(schema == 11){ // location schema1
				for(int f=0;f<fileNum;f++){
					TableInsertLocationS1 inserter = new TableInsertLocationS1();
					File dir = new File(fileDir);
					int batchNum = 100;
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName,batchNum);	
					System.out.println("finish the file"+fileName);
				}	
			}else if(schema == 111){ // location schema1
				for(int f=0;f<fileNum;f++){
					TableInsertLocationS11 inserter = new TableInsertLocationS11();
					File dir = new File(fileDir);
					int batchNum = 100;
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName,batchNum);	
					System.out.println("finish the file"+fileName);
				}	
			}else if(schema == 112){ // location schema1
				for(int f=0;f<fileNum;f++){
					TableInsertLocationS12 inserter = new TableInsertLocationS12();
					File dir = new File(fileDir);
					int batchNum = 100;
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName,batchNum);	
					System.out.println("finish the file"+fileName);
				}	
			}else if(schema == 113){ // location schema1
				for(int f=0;f<fileNum;f++){
					TableInsertLocationS13 inserter = new TableInsertLocationS13();
					File dir = new File(fileDir);
					int batchNum = 100;
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName,batchNum);	
					System.out.println("finish the file"+fileName);
				}	
			}else if(schema == 114){ // location schema1
				for(int f=0;f<fileNum;f++){
					TableInsertLocationS14 inserter = new TableInsertLocationS14();
					File dir = new File(fileDir);
					int batchNum = 100;
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName,batchNum);	
					System.out.println("finish the file"+fileName);
				}	
			}else if(schema == 115){ // location schema1
				for(int f=0;f<fileNum;f++){
					TableInsertLocationS15 inserter = new TableInsertLocationS15();
					File dir = new File(fileDir);
					int batchNum = 100;
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName,batchNum);	
					System.out.println("finish the file"+fileName);
				}	
			}else if(schema == 116){ // location schema1
				for(int f=0;f<fileNum;f++){
					TableInsertLocationS16 inserter = new TableInsertLocationS16();
					File dir = new File(fileDir);
					int batchNum = 100;
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName,batchNum);	
					System.out.println("finish the file"+fileName);
				}	
			}else if(schema == 12){ // location schema2									
				for(int f=0;f<fileNum;f++){
					TableInsertLocationS2 inserter = new TableInsertLocationS2();
					File dir = new File(fileDir);
					
					String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
					inserter.insert(fileName);						
					System.out.println("finish the file"+fileName);
				}				
			}else if(schema == 121){ // location schema2				
				
			for(int f=0;f<fileNum;f++){
				TableInsertLocationS21 inserter = new TableInsertLocationS21();
				File dir = new File(fileDir);
				
				String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
				inserter.insert(fileName);						
				System.out.println("finish the file"+fileName);
			}				
		}else if(schema == 122){ // location schema2							
			for(int f=0;f<fileNum;f++){
				TableInsertLocationS22 inserter = new TableInsertLocationS22();
				File dir = new File(fileDir);			
				String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
				inserter.insert(fileName);						
				System.out.println("finish the file"+fileName);
			}	   
	    }else if(schema == 123){
			for(int f=0;f<fileNum;f++){
				TableInsertLocationS23 inserter = new TableInsertLocationS23();
				File dir = new File(fileDir);			
				String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
				inserter.insert(fileName);						
				System.out.println("finish the file"+fileName);
			}				
		}else if(schema == 124){
			for(int f=0;f<fileNum;f++){
				TableInsertLocationS24 inserter = new TableInsertLocationS24();
				File dir = new File(fileDir);			
				String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
				inserter.insert(fileName);						
				System.out.println("finish the file"+fileName);
			}	
		}else if(schema == 125){
			for(int f=0;f<fileNum;f++){
				TableInsertLocationS25 inserter = new TableInsertLocationS25();
				File dir = new File(fileDir);			
				String fileName = dir.getAbsolutePath() +"/"+ f+"-mockup.xml";
				inserter.insert(fileName);						
				System.out.println("finish the file"+fileName);
			}			
		}
	  }
}
