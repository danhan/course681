package bixi.hbase.upload;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Hashtable;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.hbase.client.Put;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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
		File dir = new File("data2/sub");
		int batchNum = 100;
		String fileName = dir.getAbsolutePath() + "/01_10_2010__00_00_01.xml";
		inserter.insert(fileName, batchNum);
		inserter.raster.print();
	}

	public void insert(String fileName) {
		insert(fileName, 1);
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

}
