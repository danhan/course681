package util.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
/**
 * Not finished
 * @author dan
 *
 */
public class XLog {
	private static Logger logger = Logger.getLogger("util.log.XLog");
    public static String LOG_PROP_FILE_NAME = "./conf/xlog.properties";
    private static boolean _initialized = false;
    
    public static void initialize(){
    	if(_initialized)
    		return;
    	File userLogPropFile = new File(LOG_PROP_FILE_NAME);    

    	InputStream is = null;
    	try{
    		is = new FileInputStream(userLogPropFile);
    	}catch(FileNotFoundException e){
    		e.printStackTrace();
    		is = XLog.class.getResourceAsStream(LOG_PROP_FILE_NAME);
    	}
    	
    	_initialized = true;
    	
    }
    
    public static void close(){
    	
    }
    
    public static void text(){
    	
        boolean append = true;
        //FileHandler handler = new FileHandler("my.log", append);
    }
    
}
