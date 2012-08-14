package util.log;

import java.io.FileOutputStream;
import java.io.IOException;

public class XStatLog {	
	private boolean _init = false;
	private FileOutputStream output = null;
	
	public XStatLog(String filename){
    	if(_init)
    		return;
    	try{
    		output = new FileOutputStream(filename,true);
    	}catch(IOException e){
    		e.printStackTrace();
    	}
    	_init = true;
	}
	/**
	 * write the string into the file
	 * @param str
	 */
	public void write(String str){
		try{
			output.write((str+"\n").getBytes());	
		}catch(IOException e){
			e.printStackTrace();
		}		
	}
	
	/**
	 * close the file 
	 */
	public void close(){
		try{
			output.close();	
		}catch(IOException e){
			e.printStackTrace();
		}		
	}
	
	public static void main(String args[]){
	    XStatLog log = new XStatLog("test.log");	    
		log.write("12ddddd 123 44 44");
		log.write("12 123 44 44");		
		log.close();
		
	    XStatLog log1 = new XStatLog("tes1t.log");	    
	    log1.write("dd 123 44 44");
	    log1.write("12 123 44 44");		
	    log1.close();		
		
	}
	
	
}
