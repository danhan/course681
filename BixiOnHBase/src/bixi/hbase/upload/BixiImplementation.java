package bixi.hbase.upload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author hv
 *
 */
public class BixiImplementation extends BaseEndpointCoprocessor implements
    BixiProtocol {

  static final Log log = LogFactory.getLog(BixiImplementation.class);
  private static byte[] colFamily = "Data".getBytes();
  private final static String BIXI_DELIMITER = "#";
  private final static int BIXI_DATA_LENGTH = 10;

  @Override
  public Map<String, Integer> giveAvailableBikes(long milliseconds,
      List<String> stationIds, Scan scan) throws IOException {
    // scan has set the time stamp accordingly, i.e., the start and end row of
    // the scan.
    for (String qualifier : stationIds) {
      log.debug("adding qualifier: "+qualifier);
      scan.addColumn(colFamily, qualifier.getBytes());
    }
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    List<KeyValue> res = new ArrayList<KeyValue>();
    Map<String, Integer> result = new HashMap<String, Integer>();
    boolean hasMoreResult = false;
    try {
      do {
      hasMoreResult = scanner.next(res);
      for (KeyValue kv : res) {
        log.debug("got a kv: "+kv);
        int availBikes = getFreeBikes(kv);
        String id = Bytes.toString(kv.getQualifier());
        log.debug("result to be added is: "+availBikes + " id: "+ id);
        result.put(id, availBikes);
      }
      res.clear();
      } while (hasMoreResult);
    } finally {
      scanner.close();
    }
    return result;
  }

  private int getFreeBikes(KeyValue kv) {
    String availBikes = processKV(kv, 8);
    try {
      return Integer.parseInt(availBikes);
    } catch (Exception e) {
      System.err.println("Non numeric value as avail bikes!");
    }
    return 0;
  }

  private String processKV(KeyValue kv, int index) {
    if (kv == null || index > 10 || index < 0)
      return null;

    String[] str = Bytes.toString(kv.getValue()).split(
        BixiImplementation.BIXI_DELIMITER);
    // malformed value (shouldn't had been here.
    if (str.length != BixiImplementation.BIXI_DATA_LENGTH)
      return null;
    return str[index];
  }
}
