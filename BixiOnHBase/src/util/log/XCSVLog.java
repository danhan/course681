package util.log;

import java.io.FileOutputStream;
import java.util.List;

public class XCSVLog {

	private boolean _init = false;
	private FileOutputStream output = null;
	
	public XCSVLog(String filename, String[] header){
		if(_init)
			return;
		
		try{
			output = new FileOutputStream(filename,true);
			String headerStr = this.serializedHeader(header);			
			this.write(headerStr);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		this._init = true;
	}
	
	
	public void write(String str){
		try{
			this.output.write((str+"\n").getBytes());
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void close(){
		try{			
			this.output.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param header
	 * @return
	 */
	private String serializedHeader(String[] header){
		String str = "";
		for(int i=0;i<header.length;i++){
			str += header[i]+",";
		}
		return str;
	}
	
	
}
