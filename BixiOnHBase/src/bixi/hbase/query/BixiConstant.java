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
        
  
	public static String SCHEMA1_TABLE_NAME = "BixiData";
	public static String SCHEMA1_FAMILY_NAME = "Data";  
	
	public static String SCHEMA2_CLUSTER_TABLE_NAME = "Station_Cluster";//"schema2_cluster";
	public static String SCHEMA2_CLUSTER_FAMILY_NAME = "stations";
	public static String SCHEMA2_BIKE_TABLE_NAME = "Station_Statistics";//"schema2_bike";
	public static String SCHEMA2_BIKE_FAMILY_NAME = "statistics";
	
	public static String SCHEMA2_dFOF_CLUSTER_TABLE_NAME = "dfof_cluster";
	public static String SCHEMA2_dFOF_CLUSTER_FAMILY_NAME = "s";
	
	public static String ID_DELIMITER = "#";
	
	/***********************add for schema3 to verify the version*********************/
	public static String TABLE_NAME_3 = "bixi.3";
	public static String FAMILY_NAME_STATIC = "s";
	public static String FAMILY_NAME_DYNAMIC = "d";
	public static int FAMILY_NAME_STATIC_VERSION = 1;
	public static int FAMILY_NAME_3_DYNAMIC_VERSION = 60; //detail to hour:
	
	public static String TABLE_NAME_4 = "bixi.4";
	public static int FAMILY_NAME_4_DYNAMIC_VERSION = 1440; //   detail to day: 24*60 = 1440
	
	public static String s_metrics[] = { "name", "terminalName", "lat", "long", "installed", "locked", "installedDate", "temporary",  };
	public static String d_metrics[] = {"nab","ned"};
	

  
  
}
