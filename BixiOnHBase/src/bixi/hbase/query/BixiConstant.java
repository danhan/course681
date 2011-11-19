package bixi.hbase.query;
/**
 * A class containing constants for client side Bixi.
 * @author hv
 */
public class BixiConstant {
  /**
   * 
   */
  public final static double MONTREAL_LAT = 45.508183d;
  
  /**
   * 
   */
  public final static double MONTREAL_LON = -73.554094d;
  
  public final static String ID_DELIMITER = "#"; 
  
  public final static byte[] FAMILY = "Data".getBytes();
}
