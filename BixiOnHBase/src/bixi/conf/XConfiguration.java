package bixi.conf;

import java.util.Properties;

public class XConfiguration {

	private String fileName = "bixi/conf/conf.properties";
	private static XConfiguration instance = null;
	Properties configFile = new Properties();
	
	private XConfiguration(){		
		try{
			System.out.println(fileName);
			configFile.load(this.getClass().getClassLoader().getResourceAsStream(fileName));
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	public static XConfiguration getInstance(){
		if(instance == null){
			instance = new XConfiguration();
		}
		return instance;
			
	}
	
	public String getProperty(String key){
		if(this.configFile.containsKey(key)){			
			return (String)this.configFile.getProperty(key);	
		}else{						
			return null;
		}		
	}
	
}
