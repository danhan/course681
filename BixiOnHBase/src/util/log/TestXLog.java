package util.log;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class TestXLog {

	public static void main(String args[]){
		try {
		    // Create a file handler that write log record to a file called my.log
		    FileHandler handler = new FileHandler("my.log");

		    // Add to the desired logger
		    Logger logger = Logger.getLogger("com.mycompany");
		    
		    logger.addHandler(handler);
		    logger.warning("this is a test");
		} catch (IOException e) {
		}
	}
}
