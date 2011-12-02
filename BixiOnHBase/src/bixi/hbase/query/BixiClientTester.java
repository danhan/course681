package bixi.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.BixiClient;
import org.apache.hadoop.hbase.coprocessor.BixiImplementation;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A bixi client tester class. It will invoke the BixiClient class present in
 * the hbase package. 01_10_2010__00
 */
public class BixiClientTester {

  static private Configuration conf = HBaseConfiguration.create();

  public BixiClientTester() throws IOException {
  }

  public static void main(String[] args) throws Throwable {
    if (args == null || args.length < 1) {
      System.err
          .println("Wrong usage. java <classname> <Name of method to invoke>");
      System.err
          .println("1 ==> getAvailBikes, and give list of ids with # as delimitor, and a date as 01_10_2010__00");
      System.err
          .println("2 ==> getAvgUsageForAHr, a date as 01_10_2010__00, and give list of ids with # as delimitor");
      System.err
          .println("3 ==> getAvailableBikesFromAPoint, and latitude, longitude, radius and date: 45.508183d, -73.554094d, 3d, 01_10_2010__01");
      System.err
          .println("13 ==> getAvailableBikesFromAPointWithScan, and latitude, longitude, radius and date: 45.508183d, -73.554094d, 3d, 01_10_2010__01");

      System.err
          .println("12 ==> getAvgUsageForAHrWithScan, and a date as 01_10_2010__00, scan batch size, and give list of ids with # as delimitor");

      System.exit(0);
    }
    BixiClientTester tester = new BixiClientTester();
    tester.test(args);

  }

  private void test(String[] args) throws Throwable {
	  
    List<String> l = new ArrayList<String>();
    String[] idStr = null;
    
    long start_time = 0,end_time = 0;
    start_time = System.currentTimeMillis();
    if ("1".equals(args[0])){    	
    	callAvailBikes(args); // get available bike for one station
    }else if ("2".equals(args[0])){
    	callAverage(args); // get average usage for hours for a list of stations
    }else if (("3").equals(args[0])){
    	callAvailBikesFromAPoint(args); // get available bikes with a given points
    }else if (("13").equals(args[0])){
    	callAvailBikesFromAPointWithScan(args); // get available bikes with a given points
    }else if (("12").equals(args[0])){
    	callAverageAvailBikesWithScan(args); // get 
    }
    
	end_time = System.currentTimeMillis();
	System.out.println("execution time : "+ (end_time-start_time));

    // BixiClient client = new BixiClient(conf);
    // Map<String, Integer> availBikesMap = client.getAvailBikes(l, args[0]);
    // // System.out.println(availBikesMap.toString());
    // Map<String, Integer> avgUsage = client.getAvgUsageForAHr(l, args[0]);
    // // System.out.println(avgUsage.toString()); 01_10_2010__00
    // 01_10_2010__01_00
    // Map<String, Double> availBikesFromAPoint = client
    // .getAvailableBikesFromAPoint(45.508183d, -73.554094d, 3d,
    // "01_10_2010__01");
    //
    // System.out.println("availBikes: " + availBikesFromAPoint);
  }

  BixiClient client = new BixiClient(conf);

  private void callAvailBikesFromAPoint(String[] s) throws IOException,
      Throwable {
    System.out.println("callAvailBikesFromAPoint");
    if (s.length != 5)
      System.err.println("Error! Must be 5 params");
    double lat = Double.parseDouble(s[1]);
    double lon = Double.parseDouble(s[2]);
    double rad = Double.parseDouble(s[3]);
    Map<String, Double> availBikesFromAPoint = client
        .getAvailableBikesFromAPoint(lat, lon, rad, s[4]);
    System.out.println("availBikes is: " + availBikesFromAPoint);
  }

  private void callAverage(String[] s) throws IOException, Throwable {
    /*
     * if (args[1] != null && args[1].contains(BixiConstant.ID_DELIMITER)) {
     * idStr = args[1].split(BixiConstant.ID_DELIMITER); for (String id : idStr)
     * { l.add(id); } }
     */
    /**
     * [2, startDate, endDate, List]
     */
    List<String> l = new ArrayList<String>();
    if (s.length < 3) {
      System.err.println("Error! must be 3");
      return;
    }
    String ids = s[2], sDate = s[1];
    if (!("All".equals(ids))) {
      String[] idStr = ids.split(BixiConstant.ID_DELIMITER);
      for (String id : idStr) {
        l.add(id);
      }
    }
   //Map<String, Integer> avgUsage = client.getAvgUsageForAHr(l, sDate);
    //System.out.println("Average Usage: " + avgUsage);  
    
  }

  private void callAvailBikes(String s[]) throws Throwable {
    List<String> stationIds = new ArrayList<String>();
    String ids = s[1], date = s[2];
    String[] idStr = ids.split(BixiConstant.ID_DELIMITER);
    for (String id : idStr) {
      stationIds.add(id);
    }
    Map<String, Integer> m = client.getAvailBikes(stationIds, date);
    System.out.println("avail bikes: " + m);
  }

  /**
   * A scan method that will fetch the row from the RS and compute the distance
   * of the points from the given ones.
   * @param s
   * @throws IOException
   * @throws Throwable
   */
  private void callAvailBikesFromAPointWithScan(String[] s) throws IOException,
      Throwable {
    System.out.println("callAvailBikesFromAPointWithScan");
    if (s.length != 5)
      System.err.println("Error! Must be 5 params");
    
    s[1]="45.52830025";
    s[2]="-73.526967";
    s[3]="7";
    s[4]="01_10_2010__03";
    
    double lat = Double.parseDouble(s[1]);
    double lon = Double.parseDouble(s[2]);
    double rad = Double.parseDouble(s[3]);
    String dateWithHr = s[4];

    Get g = new Get(Bytes.toBytes(dateWithHr + "_00"));
    System.out.println(dateWithHr + "_00");
    HTable table = new HTable(conf, "BixiData".getBytes());
    Result r = table.get(g);
    Map<String, Double> result = new HashMap<String, Double>();
    // this r contains the entire row for the hr+00 min. Now compute the
    // distance and do the stuff.
    String valStr = null, latStr = null, lonStr = null;
    for (KeyValue kv : r.raw()) {
      valStr = Bytes.toString(kv.getValue());
      // log.debug("cell value is: "+s);
      String[] sArr = valStr.split(BixiConstant.ID_DELIMITER); // array of
      // key=value
      // pairs
      latStr = sArr[3];
      lonStr = sArr[4];
      latStr = latStr.substring(latStr.indexOf("=") + 1);
      lonStr = lonStr.substring(lonStr.indexOf("=") + 1);
      // log.debug("lon/lat values are: "+lonStr +"; "+latStr);
      double distance = giveDistance(Double.parseDouble(latStr), Double
          .parseDouble(lonStr), lat, lon)
          - rad;
      // log.debug("distance is : "+ distance);      
      if (distance < 0) {// with in the distance: add it
        result.put(sArr[0], distance);
      }
    }

    // Map<String, Double> availBikesFromAPoint = client
    // .getAvailableBikesFromAPoint(lat, lon, rad, s[4]);
    System.out.println("availBikes is with Scan: " + result);
  }

  final static double RADIUS = 6371;

  private double giveDistance(double lat1, double lon1, double lat2, double lon2) {
    double dLon = Math.toRadians(lon1 - lon2);
    double dLat = Math.toRadians(lat1 - lat2);
    double a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(Math.toRadians(lat1))
        * Math.cos(Math.toRadians(lat2)) * Math.pow(Math.sin(dLon / 2), 2);
    double res = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    double distance = RADIUS * res;
    return distance;
  }

  /**
   * s shd have: [12, sDate<10_10_2010__12>, eDate<10_10_2010__15>, scan-batch size<60>, list of
   * ids<12#123#>,]
   * @param s
   * @throws IOException
   */
  private void callAverageAvailBikesWithScan(String s[]) throws IOException {
    if (s.length < 5) {
      System.err.println("length shd be greater than 3");
      return;
    }
    List<String> stationIds = new ArrayList<String>();
    String ids = s[4], sDateWithHour = s[1], eDateWithHour = s[2];    
    
    ids = "All";
    sDateWithHour = "00_10_2010__59";
    eDateWithHour = "02_10_2010__00";
   
    int catchSize = Integer.parseInt(s[3]);
    catchSize = 1000;
    if (!("All").equals(ids)) {
      String[] idStr = ids.split(BixiConstant.ID_DELIMITER);
      for (String id : idStr) {
        stationIds.add(id);
      }
    }
    Scan scan = new Scan();
    scan.setCaching(catchSize);
    if (sDateWithHour != null && eDateWithHour != null) {
      scan.setStartRow((sDateWithHour + "_00").getBytes());
      scan.setStopRow((eDateWithHour + "_59").getBytes());
    }

    for (String qualifier : stationIds) {
      scan.addColumn(BixiConstant.FAMILY, qualifier.getBytes());
    }
    HTable table = new HTable(conf, "BixiData".getBytes());
    Map<String, Integer> result = new HashMap<String, Integer>();
    
    long starttime = System.currentTimeMillis();
    ResultScanner scanner = table.getScanner(scan);
    int counter = 0;
    try {

      for (Result r : scanner) {       
        //System.out.println("Row number:"+counter);
        for (KeyValue kv : r.raw()) {
          int emptyDocks = getEmptyDocks(kv);
          String id = Bytes.toString(kv.getQualifier());
          Integer prevVal = result.get(id);
          emptyDocks = emptyDocks + (prevVal != null ? prevVal.intValue() : 0);
     //     System.out.println("result to be added is: " + emptyDocks + " id: "
       //       + id);
          result.put(id, emptyDocks);
          counter++;
        }
      }
    } finally {
      scanner.close();
    }
    
    System.out.println("counter: "+counter + "; time = "+ (System.currentTimeMillis()-starttime));
    
    for (Map.Entry<String, Integer> e : result.entrySet()) {
    //  System.out.println("counter and value is" + counter + ","+e.getKey()+": " + e.getValue());
      int i = e.getValue() / counter;
      result.put(e.getKey(), i);
    }
    System.out.println("Avg map is: " + result);
    // return result;
  }

  private int getEmptyDocks(KeyValue kv) {
    String strVal = Bytes.toString(kv.getValue());
    String[] str = Bytes.toString(kv.getValue()).split(
        BixiConstant.ID_DELIMITER);
    if (str.length != 11)
      return 0;
    String availBikes = str[10];
   // System.out.println("emptyDocks::" + availBikes);
    try {
      return Integer
          .parseInt(availBikes.substring(availBikes.indexOf("=") + 1));
    } catch (Exception e) {
      System.err.println("Non numeric value as avail bikes!");
    }
    return 0;
  }
}
