package bixi.dataset.cluster;

import bixi.dataset.collection.XStation;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;

/*
 * This is from Wei
 */
public class BixiReaderToTable 
{
	//added by Wei
	private static String dstTable = "test1001";

	//commented out by Wei
	public static List<XStation> stationList = null;
	private static DocumentBuilderFactory dbf = null;
	private static DocumentBuilder db = null;
	
	
	//added by Wei
	private static HTable hTab = null;
	
	public BixiReaderToTable() throws IOException
	{
		try{
			dbf = DocumentBuilderFactory.newInstance();
			db = dbf.newDocumentBuilder();
			stationList = new LinkedList<XStation>();
			
		}catch(ParserConfigurationException e){
			e.printStackTrace();
		}
		
		//added by Wei
		// This is commented because the upgrade
		//hTab = new HTable(Bytes.toBytes(dstTable));
	}
	
	public BixiReaderToTable(String dstTable) throws IOException
	{
		this.dstTable = dstTable;
		
		try{
			dbf = DocumentBuilderFactory.newInstance();
			db = dbf.newDocumentBuilder();
			stationList = new LinkedList<XStation>();
		}catch(ParserConfigurationException e){
			e.printStackTrace();
		}
		
		//added by Wei
		// This is commented because the upgrade
		//hTab = new HTable(Bytes.toBytes(dstTable));
	}
	
	public static void main(String[] argc) throws IOException
	{

//		try{
//			dbf = DocumentBuilderFactory.newInstance();
//			db = dbf.newDocumentBuilder();
//			stationList = new LinkedList<XStation>();
//		}catch(ParserConfigurationException e){
//			e.printStackTrace();
//		}
		BixiReaderToTable write_to_table = new BixiReaderToTable();
		
		
		File dir = new File("data2");
		
		System.out.println(dir.getAbsolutePath());
		
		String filename = dir.getAbsolutePath() + "/01_10_2010__00_00_01.xml";
		
		System.out.println(filename);

		parseXML(filename, "test1001");
		
	}
	
	/*
	 * fileName includes full path of the file and the file name
	 */
	public static List<XStation> parseXML(String fileFullName, String dstTable) throws IOException 
	{

		BixiReaderToTable.dstTable = dstTable;	
		
		try{
			dbf = DocumentBuilderFactory.newInstance();
			db = dbf.newDocumentBuilder();
			stationList = new LinkedList<XStation>();
		}catch(ParserConfigurationException e){
			e.printStackTrace();
		}
		
		File f = new File(fileFullName);		
		Document dom = null;
		try {
			dom = db.parse(f);
		} catch (SAXException e) {
			e.printStackTrace();
			System.err.println("File is malformed:" + fileFullName);
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("File is malformed:" + fileFullName);
		}
		if (dom != null) {
			Element elem = dom.getDocumentElement();	

			NodeList nodes = elem.getElementsByTagName("station");				

			Element e;	

			for (int i = 0; i < nodes.getLength(); i++) {
				try {								
					e = (Element) nodes.item(i);
					XStation station = getStation(e);
					
					//commented out by Wei
					//stationList.add(station);
					
					//changed by Wei
					addStationToTable(station);
					
				} catch (Exception e1) {
					e1.printStackTrace();
					System.err.println("Malformed Station ID data");
					throw new IOException();
				}				
			}
		}
		return stationList;
	}
	
	//added by Wei
	private static void addStationToTable(XStation station) throws IOException
	{
		Put put = new Put(Bytes.toBytes(station.getId()));
		
		/*
		put.add(Bytes.toBytes("station"), Bytes.toBytes("id"), Bytes.toBytes(station.getId()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("name"), Bytes.toBytes(station.getName()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("terminalName"), Bytes.toBytes(station.getTerminalName()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("latitude"), Bytes.toBytes(station.getLatitude()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("longitude"), Bytes.toBytes(station.getlongitude()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("installed"), Bytes.toBytes(station.isInstalled()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("locked"), Bytes.toBytes(station.isLocked()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("installDate"), Bytes.toBytes(station.getInstallDate()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("removeDate"), Bytes.toBytes(station.getRemoveDate()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("temporary"), Bytes.toBytes(station.isTemporary()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("temporary"), Bytes.toBytes(station.isTemporary()));
		*/

		
		put.add(Bytes.toBytes("station"), Bytes.toBytes("sid"), Bytes.toBytes(String.valueOf(station.getId())));
		
		put.add(Bytes.toBytes("station"), Bytes.toBytes("latitude"), Bytes.toBytes(String.valueOf(station.getLatitude())));
		//System.out.println("latitude: " + station.getLatitude());
		
		put.add(Bytes.toBytes("station"), Bytes.toBytes("longitude"), Bytes.toBytes(String.valueOf(station.getlongitude())));
		//System.out.println("longitude: " + station.getlongitude());
		
		put.add(Bytes.toBytes("station"), Bytes.toBytes("metadata"), Bytes.toBytes(String.valueOf(station.getMetadata())));
		//System.out.println("metadata: " + station.getMetadata());
		
		hTab.put(put);
		
		
		//for test use
		/*
		String metadatapp = station.getId() + ";" + station.getMetadata();
		
		System.out.println("metadatapp: " + metadatapp);
		
		
		
		int index = metadatapp.indexOf(';');
		
		String id2 = metadatapp.substring(0, index);
		String metadata2 = metadatapp.substring(index + 1, metadatapp.length());
		
		
		System.out.println("id2: " + id2);
		System.out.println("metadata2: " + metadata2);
		
		System.out.println(station.getId().equals(id2));
		System.out.println(station.getMetadata().equals(metadata2));
		
		 */
	}
	
	private static String seperateIdMetadata(String metadatapp, String metadata)
	{
		int index = metadatapp.indexOf(';');
		
		String id = metadatapp.substring(0, index);
		
		metadata = metadatapp.substring(index + 1, metadatapp.length());
		
		return id;
	}
	
	private static XStation getStation(Element item) {
		Field[] fields = XStation.class.getDeclaredFields();		
		XStation station = new XStation();			
		for (int i=0;i<fields.length;i++) {				
			String name = fields[i].getName();
			if (name.contains("lat")){
				name = "lat";
			}else if(name.contains("long")){
				name = "long";
			}
			NodeList nodes = item.getElementsByTagName(name);			
			Element ele = (Element) nodes.item(0);

			if(ele != null && ele.getFirstChild() != null){
				String value = ele.getFirstChild().getNodeValue();				

				//System.out.println(name+"==>"+value);
				if (name.contains("lat")) {
					station.setLatitude(Double.valueOf(value).doubleValue());
				} else if (name.contains("long")) {
					station.setlongitude(Double.valueOf(value).doubleValue());
				} else if (name.contains("id")) {
					station.setId(value);
				} else if (name.contains("name")) {
					station.setName(value);
				} else if (name.contains("terminalName")) {
					station.setTerminalName(value);
				} else if (name.contains("installed")) {
					station.setInstalled(Boolean.valueOf(value).booleanValue());
				} else if (name.contains("locked")) {
					station.setLocked(Boolean.valueOf(value).booleanValue());
				} else if (name.contains("temporary")) {
					station.setTemporary(Boolean.valueOf(value).booleanValue());
				} else if (name.contains("nbBikes")) {
					station.setNbBikes(Integer.valueOf(value).intValue());
				} else if (name.contains("nbEmptyDocks")) {
					station.setNbEmptyDocks(Integer.valueOf(value).intValue());
				} else if (name.contains("installDate")) {
					station.setInstallDate(Long.valueOf(value).longValue());
				} else if (name.contains("removeDate")) {
					station.setRemoveDate(Long.valueOf(value).longValue());
				}				
			}

		}
		return station;

	}
	
}
