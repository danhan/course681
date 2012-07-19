package bixi.hbase.upload;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Hashtable;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.hbase.client.Put;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import util.quadtree.based.trie.XQuadTree;
import util.raster.XBox;
import util.raster.XRaster;

import bixi.dataset.collection.XStation;
import bixi.hbase.query.BixiConstant;

/**
 * This schema is to split the space into Raster, then it can be stored into a
 * matrix. Schema2: It is got the index by the split the space in a way of
 * matrix, it determined by the stride of the row, and the number of the
 * columns, so there would be more than one object locating in the same box(row
 * and column), so the version dimension is to contain all objects in the same
 * box Key(the index of the row )e.g(0000, 0001, 0002,0003) Columns(the index of
 * the column) e.g (0000,0001,0002,0003) Version(the number of the objects)
 * Values(static information of stations)
 * 
 * @author dan
 * 
 */
public class TableInsertLocationS2 extends TableInsertAbstraction {



	private XRaster raster = null;

	/**
	 * This should be known before indexing with Raster
	 */
	double min_size_of_height = BixiConstant.MIN_SIZE_OF_SUBSPACE;

	/**
	 * This should be known before indexing with Raster
	 */
	int num_of_column = BixiConstant.MAX_NUM_OF_COLUMN;

	public TableInsertLocationS2() throws IOException {
		super();
		this.tableName = BixiConstant.LOCATION_TABLE_NAME_2;
		this.familyName = BixiConstant.LOCATION_FAMILY_NAME;
		try{
			this.setHBase();	
		}catch(Exception e){
			e.printStackTrace();
		}
		// build the Raster for the space
		raster = new XRaster(space, min_size_of_height, num_of_column);		
	}

	public static void main(String[] args) throws ParserConfigurationException,
			IOException {
		TableInsertLocationS2 inserter = new TableInsertLocationS2();
		File dir = new File("data2");
		//int batchNum = 100;
		String fileName = dir.getAbsolutePath() +"/"+ args[0];
		inserter.insert(fileName);
		inserter.raster.print();
	}

	public void insert(String fileName) {
		//insert(fileName, 1);
		long start = System.currentTimeMillis();
		insertSAX(fileName);
		System.out.println("time=>"+(System.currentTimeMillis()-start));
	}

	@Override
	public void insert(String fileName, int batchNum) {
		try {
			File f = new File(fileName);
			Document dom = null;
			try {
				DocumentBuilderFactory dbf = DocumentBuilderFactory
						.newInstance();
				DocumentBuilder db = dbf.newDocumentBuilder();
				dom = db.parse(f);
			} catch (SAXException e) {
				e.printStackTrace();
				System.err.println("File is malformed:" + fileName);
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("File is malformed:" + fileName);
			}
			if (dom != null) {
				Element elem = dom.getDocumentElement();
				NodeList nodes = elem.getElementsByTagName("station");
				Element e;
				for (int i = 0; i < nodes.getLength(); i++) {
					try {
						e = (Element) nodes.item(i);
						// get the location
						XStation station = reader.getStation(e);
						// index the location
						XBox box = raster.addPoint((float) station.getLatitude(),
								(float) Math.abs(station.getlongitude()));

						String key = box.getRow();						
						// insert it into hbase
						Put put = new Put(key.getBytes());
						put.add(familyName.getBytes(), box.getColumn()
								.getBytes(), box.getObjectCount(), station.getFullMetadata().getBytes());
						System.out.println(box.toString()+":=>"+station.getLatitude()+";"+station.getlongitude());
						this.hbase.getHTable().put(put);
						
					} catch (Exception e1) {
						e1.printStackTrace();
						System.err.println("Malformed Station ID data");
						throw new IOException();
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.hbase.closeTableHandler();
		}

	}
	
	/**
	 * Parse the xml file with JAX
	 * Read the location from file, parse it, index it, and then insert it
	 * TODO Normalize the point: enlarge the width and height, so all the points are in the scope now.
	 * @param filename
	 *            original file including all location information
	 * @param batchNum
	 *            how many rows can be written at one go
	 */
	public void insertSAX(String filename) {
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
		
			
			DefaultHandler handler = new DefaultHandler(){
				boolean bID = false;
				boolean bName = false;
				boolean bLat = false;
				boolean bLong = false;
				XStation station = new XStation();
				int count = 0;
				
			public void startElement(String uri, String localName, String qName, 
					Attributes attributes) throws SAXException{
				
				//System.out.println("start element: " + qName);
				if(qName.equalsIgnoreCase("id")){
					bID = true;
				}else if(qName.equalsIgnoreCase("terminalName")){
					bName = true;
				}else if(qName.equalsIgnoreCase("lat")){
					bLat = true;
				}else if(qName.equalsIgnoreCase("long")){
					bLong = true;
				}								
			}
			
			public void endElement(String uri, String localName, String qName) throws SAXException{
				//System.out.println("end Element: "+qName);
				if(qName.equalsIgnoreCase("station")){
					// index the location
					XBox box = raster.addPoint((float) station.getLatitude(),
							(float) Math.abs(station.getlongitude()));

					String key = box.getRow();						
					
					// insert it into hbase
					try{
						// insert it into hbase
						Put put = new Put(key.getBytes());
						put.add(familyName.getBytes(), box.getColumn()
								.getBytes(), box.getObjectCount(), station.getFullMetadata().getBytes());
						//System.out.println(box.toString()+":=>"+station.getLatitude()+";"+station.getlongitude());
						hbase.getHTable().put(put);	
						count++;
					}catch(Exception e){
						e.printStackTrace();
					}

				}else if (qName.equalsIgnoreCase("stations")){
					System.out.println("insert number: "+count);
				}
			}
			
			public void characters(char ch[], int start, int length) throws SAXException{
				
				String character = new String(ch, start, length);				
				if(bID){
					bID = false;
					station.setId(character);
				}else if(bName){
					bName = false;
					station.setName(character);
				}else if(bLat){
					bLat = false;
					station.setLatitude(Double.valueOf(character.trim()));
				}else if(bLong){
					bLong = false;
					station.setlongitude(Double.valueOf(character.trim()));
				}
			}
		};	
			saxParser.parse(filename, handler);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.hbase.closeTableHandler();
			
		}
	}


}
