package bixi.query.coprocessor;

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

  Map<String, TotalNum> giveTotalUsage(List<String> stationIds, Scan scan)
      throws IOException;

  Map<String, Integer> getAvailableBikesFromAPoint(double lat, double lon,
      double radius, Get get) throws IOException;
  
  Map<String, TotalNum> getTotalUsage_Schema2(Scan scan) throws IOException;

  Map<String, Integer> getAvailableBikesFromAPoint_Schema2(Scan scan) throws IOException;
  
  List<String> getStationsNearPoint_Schema2(double lat, double lon) throws IOException;
  
  /**************************For Time Schema3************************/

  Map<String, TotalNum> copGetTotalUsage4S3(Scan scan) throws IOException;
  
  /*******************For location Schema1**************************/
  List<String> copQueryNeighbor4LS1(Scan scan,double latitude,double longitude,double radius)throws IOException;
  
  /*******************For location Schema2**************************/
  List<String> copQueryNeighbor4LS2(Scan scan,double latitude,double longitude,double radius)throws IOException;
  
  
}