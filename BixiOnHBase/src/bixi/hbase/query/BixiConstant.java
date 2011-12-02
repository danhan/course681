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
      
  public final static byte[] FAMILY = "Data".getBytes();
  
	public static String SCHEMA1_TABLE_NAME = "BixiData";
	public static String SCHEMA1_FAMILY_NAME = "Data";  
	
	public static String SCHEMA2_CLUSTER_TABLE_NAME = "Station_Cluster";//"schema2_cluster";
	public static String SCHEMA2_CLUSTER_FAMILY_NAME = "stations";
	public static String SCHEMA2_BIKE_TABLE_NAME = "Station_Statistics";//"schema2_bike";
	public static String SCHEMA2_BIKE_FAMILY_NAME = "statistics";
	
	public static String SCHEMA2_dFOF_CLUSTER_TABLE_NAME = "dfof_cluster";
	public static String SCHEMA2_dFOF_CLUSTER_FAMILY_NAME = "s";
	
	public static String ID_DELIMITER = "#";
  
  
}
