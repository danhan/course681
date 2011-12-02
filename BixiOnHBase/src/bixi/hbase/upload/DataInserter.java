package bixi.hbase.upload;


import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Takes the raw xmls and insert it into HBase. Currently there are files
 * already in the fs, so upload them first.
 */
public class DataInserter {
  static String[] keyArr = { "id", "name", "terminalName", "lat", "long",
      "installed", "locked", "installedDate", "temporary", "nbBikes",
      "nbEmptyDocks" }; /// 11 elements

  static Configuration conf = HBaseConfiguration.create();
  HTable table;
  static byte[] tableName = "BixiData".getBytes();
  static byte[] colFamily = "Data".getBytes();
  
  /**
   * @throws IOException
   */
  public DataInserter() throws IOException {
    table = new HTable(conf, tableName);    
    table.setAutoFlush(true);
  }

  public static void main(String[] args) throws ParserConfigurationException, IOException {
    DataInserter inserter = new DataInserter();
    String fileDir = "data2";
    inserter.insertXmlData(fileDir);
    //inserter.readData();
  }



  public void insertXmlData(String fileDir) throws ParserConfigurationException {
    
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    
    File dir = new File(fileDir);
    if (!dir.isDirectory()){
      System.out.println(" dir is: "+dir.getAbsolutePath());
      System.exit(1);
    }
    String[] fileNames = dir.list();
    for (String fileName : fileNames) { // instantiate the file and dump it
      System.out.println("processing file: "+dir.getAbsoluteFile() + "/" + fileName);
      File f = new File(dir.getAbsoluteFile() + "/" + fileName);
      if(f.length() < 1024*5) { // < 5k
        System.err.println("File is corrupt!" + f.getAbsolutePath());
        continue;// erroreneous file
      }
      try {
        Document dom = db.parse(f);
        this.processDoc(dom, fileName);
      } catch (SAXException e) {
        e.printStackTrace();
        System.err.println("File is malformed:"+fileName);
      } catch (IOException e) {
        e.printStackTrace();
        System.err.println("File is malformed:"+fileName);
      }
    }

  }

  private void processDoc(Document dom, String fileName) throws IOException {
    //System.out.println(dom.toString());// dom.getDocumentElement().toString();
    Element elem = dom.getDocumentElement();
    //System.out.println(elem.getNodeName().toString());
    String key = fileName.substring(0, fileName.lastIndexOf("_"));
    Put put = new Put(key.getBytes());
    NodeList stationList = elem.getElementsByTagName("station");
    Element e;
    String value, stationId;
    for (int i = 0; i < stationList.getLength(); i++){
      try {
        e = (Element) stationList.item(i);
        value = processPerStationData(e);
        stationId = value.substring(0, value.indexOf("#")); //id=433
        stationId = stationId.substring(stationId.indexOf("=")+1);
        //System.out.println(colFamily + "???" + stationId + "???" + value);
        put.add(colFamily, stationId.getBytes(), value.getBytes());
      } catch (Exception e1) {
        System.err.println("Malformed Station ID data");
        throw new IOException();
      }
      // data is good, add it to Put object
    }
    System.out.println(new String(put.getRow()));
    table.put(put);
  }

  /**
   *Store the element in a form of key=value#key=value <station> <id>1</id>
   * <name>Notre Dame / Place Jacques Cartier</name><terminalName
   * >6001</terminalName><lat>45.508183</lat><long>-73.554094
   * </long><installed>true
   * </installed><locked>false</locked><installDate>1276012920000
   * </installDate><removalDate
   * /><temporary>false</temporary><nbBikes>4</nbBikes
   * ><nbEmptyDocks>27</nbEmptyDocks></station>
   * @param item
   */
  private String processPerStationData(Element item) {
    StringBuffer sb = new StringBuffer();
    for (String s : keyArr) {
      NodeList nl = item.getElementsByTagName(s);
      Element e = (Element) nl.item(0);
      if (e != null && e.getFirstChild() != null)
        sb.append(s).append("=").append(e.getFirstChild().getNodeValue());
      if (s != "nbEmptyDocks")
        sb.append("#");
    }
    return sb.toString();
  }


  private void readData() throws IOException {
	    Get g = new Get("01_10_2010__00_29".getBytes());
	    g.addColumn(colFamily, "93".getBytes());
	    Result r = table.get(g);
	    byte [] v = r.getValue(colFamily, "93".getBytes());
	    String val = Bytes.toString(v);
	    System.out.println("is something there"+val);
	  }
  
  
}
