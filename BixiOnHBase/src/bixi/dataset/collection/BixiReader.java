package bixi.dataset.collection;

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

public class BixiReader {

	public List<XStation> stationList = null;
	private DocumentBuilderFactory dbf = null;
	private DocumentBuilder db = null;

	public BixiReader() {
		try{
			dbf = DocumentBuilderFactory.newInstance();
			db = dbf.newDocumentBuilder();
			stationList = new LinkedList<XStation>();
		}catch(ParserConfigurationException e){
			e.printStackTrace();
		}
	}

	/*
	 * fileName includes full path of the file and the file name
	 */
	public List<XStation> parseXML(String fileFullName) throws IOException {

		File f = new File(fileFullName);
		System.out.println(fileFullName);
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
			System.out.println(nodes.getLength());
			
			Element e;			
			for (int i = 0; i < nodes.getLength(); i++) {
				try {
					e = (Element) nodes.item(i);
					XStation station = this.getStation(e);
					stationList.add(station);
				} catch (Exception e1) {
					e1.printStackTrace();
					System.err.println("Malformed Station ID data");
					throw new IOException();
				}				
			}
		}
		return stationList;

	}

	private XStation getStation(Element item) {
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
					station.setLongtitude(Double.valueOf(value).doubleValue());
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