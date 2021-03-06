package bixi.hbase.query;
/**
 * A class containing constants for client side Bixi.
 * @author DH
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
  
  /*
   * The Montreal station space: ( 45.415714,-73.526967) x distance: 0.15011499999999955, y distance: 0.1639420000000058; 
   */
  
  public final static double MONTREAL_TOP_LEFT_X = 0;//45.415714;
  
  public final static double MONTREAL_TOP_LEFT_Y = 0;//73.526967;
  
  public final static double MONTREAL_AREA_WIDTH = 100;//0.16;
  
  public final static double MONTREAL_AREA_HEIGHT = 100;//0.17;
  
  /**
   * static metrics in data
   */
	public static String s_metrics[] = { "name", "terminalName", "lat", "long", "installed", "locked", "installedDate", "temporary",  };
/**
 * dynamic metrics in data
 */
	public static String d_metrics[] = {"nab","ned"};
  
	/**
	 *
	 */
	public static String SCHEMA1_TABLE_NAME = "BixiData";
	public static String SCHEMA1_FAMILY_NAME = "Data";  
	
	public static String SCHEMA2_CLUSTER_TABLE_NAME = "Station_Cluster";//"schema2_cluster";
	public static String SCHEMA2_CLUSTER_FAMILY_NAME = "stations";
	public static String SCHEMA2_BIKE_TABLE_NAME = "Station_Statistics";//"schema2_bike";
	public static String SCHEMA2_BIKE_FAMILY_NAME = "statistics";
	
	public static String SCHEMA2_dFOF_CLUSTER_TABLE_NAME = "dfof_cluster";
	public static String SCHEMA2_dFOF_CLUSTER_FAMILY_NAME = "s";
	
	public static String ID_DELIMITER = "#";
	
	/***********************add for schema3 & schema4 to verify the version*********************/
	public static String TABLE_NAME_3 = "bixi.3";
	public static String FAMILY_NAME_STATIC = "s";
	public static String FAMILY_NAME_DYNAMIC = "d";
	public static int FAMILY_NAME_STATIC_VERSION = 1;
	public static int FAMILY_NAME_3_DYNAMIC_VERSION = 60; //detail to hour:
	
	public static String TABLE_NAME_4 = "bixi.4";
	public static int FAMILY_NAME_4_DYNAMIC_VERSION = 1440; //   detail to day: 24*60 = 1440
	
	/********************For Location Schema1 and Schema2*************************************/
	public static String LOCATION_TABLE_NAME_1 = "bixi.location.1";
	public static String LOCATION_FAMILY_NAME = "f";	
	public static String LOCATION_TABLE_NAME_2 = "bixi.location.2";
	

	/********************Different Size of Subpace for two schema***************************************/
	  
	//  public final static double MIN_SIZE_OF_SUBSPACE = 0.1;//0.1;//2;//0.001;    
	  /**
	   * This should be used in Raster indexing
	   */
	//  public final static int MAX_NUM_OF_COLUMN = 1000;//1000;//10000;
	
	public final static double MIN_SIZE_OF_SUBSPACE0 = 0.1;
	public final static double MIN_SIZE_OF_SUBSPACE1 = 1;
	public final static double MIN_SIZE_OF_SUBSPACE2 = 0.01;
	public final static double MIN_SIZE_OF_SUBSPACE3 = -1;
	public final static double MIN_SIZE_OF_SUBSPACE4 = -1;
	public final static double MIN_SIZE_OF_SUBSPACE5 = -1;
	
	/*This is for Schema2, 100 means the maximum width of the whole space*/
	public final static int MAX_NUM_OF_COLUMN0 = (int)(100 / MIN_SIZE_OF_SUBSPACE0);
	public final static int MAX_NUM_OF_COLUMN1 = (int)(100 / MIN_SIZE_OF_SUBSPACE1);
	public final static int MAX_NUM_OF_COLUMN2 = (int)(100 / MIN_SIZE_OF_SUBSPACE2);
	public final static int MAX_NUM_OF_COLUMN3 = (int)(100 / MIN_SIZE_OF_SUBSPACE2);
	public final static int MAX_NUM_OF_COLUMN4 = (int)(100 / MIN_SIZE_OF_SUBSPACE2);
	public final static int MAX_NUM_OF_COLUMN5 = (int)(100 / MIN_SIZE_OF_SUBSPACE2);
  
}
