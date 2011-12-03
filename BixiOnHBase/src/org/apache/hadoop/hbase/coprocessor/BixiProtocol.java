package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
* Provides Bixi specific utilities served as a Coprocessor.
*/
public interface BixiProtocol extends CoprocessorProtocol {
  /**
* Given the time (in ms) and list of station ids, give number of free bikes.
* If list is empty, give result for all station Ids.
* @param milliseconds
* @param stationIds
* @return
* @throws IOException
*/
  Map<String, Integer> giveAvailableBikes(long milliseconds,
      List<String> stationIds, Scan scan) throws IOException;

  Map<String, Integer> giveAverageUsage(List<String> stationIds, Scan scan)
      throws IOException;

  Map<String, Integer> getAvailableBikesFromAPoint(double lat, double lon,
      double radius, Get get) throws IOException;
  
  Map<String, Integer> getTotalUsage_Schema2(Scan scan)
  throws IOException;

  Map<String, Integer> getAvailableBikesFromAPoint_Schema2(Scan scan) throws IOException;
  List<String> getStationsNearPoint_Schema2(double lat, double lon) throws IOException;

}