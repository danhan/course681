package bixi.hbase.query.location;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import util.raster.XBox;
import util.raster.XRaster;
import bixi.dataset.collection.BixiReader;
import bixi.dataset.collection.XStation;
import bixi.hbase.query.BixiConstant;
import bixi.hbase.query.QueryAbstraction;
import bixi.query.coprocessor.BixiProtocol;

/**
 * This class is to process the location query based on Location Schema2
 * 
 * @author dan
 * 
 */
public class BixiLocationQueryS2 extends QueryAbstraction {

	double min_size_of_height = BixiConstant.MIN_SIZE_OF_SUBSPACE;
	int max_num_of_column = BixiConstant.MAX_NUM_OF_COLUMN;

	public BixiLocationQueryS2() {
		tableName = BixiConstant.LOCATION_TABLE_NAME_2;
		familyName = new String[] { BixiConstant.LOCATION_FAMILY_NAME };
		try {
			this.setHBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<String> copQueryAvailableNear(String timestamp,
			final double latitude, final double longitude, final double radius) {
		
		long s_time = System.currentTimeMillis();
		try {
			/** Step1** Call back class definition **/
			class BixiCallBack implements Batch.Callback<List<String>> {
				List<String> res = new ArrayList<String>();
				int count = 0;

				@Override
				public void update(byte[] region, byte[] row,
						List<String> result) {
					System.out.println((count++) + ": come back region: "
							+ Bytes.toString(region) + "; result: "
							+ result.size());
					res.addAll(result); // to verify the error when large data
				}

			}
			BixiCallBack callBack = new BixiCallBack();

			/** Step2** generate the scan ***********/
			// build up the Raster
			XRaster raster = new XRaster(this.space, this.min_size_of_height,
					this.max_num_of_column);
			// match the query area in Raster to get the row range and column
			// range
			XBox[] match_boxes = raster.match(latitude, longitude, radius);
			String[] rowRange = new String[2];
			rowRange[0] = match_boxes[0].getRow();
			rowRange[1] = match_boxes[1].getRow();

			String[] c = raster.getColumns(match_boxes[0], match_boxes[1]);
			// generate the scan
			final Scan scan = hbaseUtil.generateScan(rowRange, null, null,
					null, -1);
			
			/** Step3** send out the query to trigger the corresponding function in Coprocessor****/
			hbaseUtil.getHTable().coprocessorExec(BixiProtocol.class,
					scan.getStartRow(), scan.getStopRow(),
					new Batch.Call<BixiProtocol, List<String>>() {

						public List<String> call(BixiProtocol instance)
								throws IOException {

							return instance.copQueryNeighbor4LS2(scan,
									latitude, longitude, radius);

						};
					}, callBack);
			
		    long e_time = System.currentTimeMillis();
		    
			long exe_time = e_time - s_time;
			// TODO store the time into database
			System.out.println("exe_time=>"+exe_time+";result=>"+callBack.res.size());				
			
			return callBack.res;

		} catch (Exception e) {
			e.printStackTrace();
		}catch(Throwable ee){
			ee.printStackTrace();
		}finally{
			hbaseUtil.closeTableHandler();
		}
		return null;

	}

	@Override
	public HashMap<String, String> scanQueryAvailableNear(String timestamp,
			double latitude, double longitude, double radius) {
		long sTime = System.currentTimeMillis();
		// build up a raster
		XRaster raster = new XRaster(this.space, this.min_size_of_height,
				this.max_num_of_column);
		Point2D.Double point = new Point2D.Double(latitude, longitude);
		ResultScanner rScanner = null;
		// return result
		HashMap<String, String> results = new HashMap<String, String>();
		try {
			// match rect to find the subspace it belongs to
			XBox[] match_boxes = raster.match(latitude, longitude, radius);
			String[] rowRange = new String[2];
			rowRange[0] = match_boxes[0].getRow();
			rowRange[1] = match_boxes[1].getRow();

			String[] c = raster.getColumns(match_boxes[0], match_boxes[1]);
			/*
			 * System.out.println(rowRange[0]+":"+rowRange[1]); for(int
			 * i=0;i<c.length;i++){ System.out.print(c[i]+";"); }
			 * System.out.println();
			 */
			// the version here is harded coded, because i cannot get how many
			// objects in one cell now
			rScanner = this.hbaseUtil.getResultSet(rowRange, null,
					this.familyName, c, 10000);
			BixiReader reader = new BixiReader();
			int count = 0;
			int accepted = 0;

			for (Result r : rScanner) {
				NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = r
						.getMap();

				for (byte[] family : resultMap.keySet()) {
					NavigableMap<byte[], NavigableMap<Long, byte[]>> columns = resultMap
							.get(family);
					for (byte[] col : columns.keySet()) {
						NavigableMap<Long, byte[]> values = columns.get(col);
						for (Long version : values.keySet()) {
							count++;
							// get the distance between this point and the given
							// point
							XStation station = reader.getStationFromJson(Bytes
									.toString(values.get(version)));

							Point2D.Double resPoint = new Point2D.Double(
									station.getLatitude(), station
											.getlongitude());
							double distance = resPoint.distance(point);

							if (distance <= radius) {
								// System.out.println("row=>"+Bytes.toString(r.getRow())
								// +
								// ";colum=>"+Bytes.toString(col)+";version=>"+version+
								// ";station=>"+station.getId()+";distance=>"+distance);
								accepted++;
								results.put(station.getId(),
										String.valueOf(distance));
							}
						}
					}
				}
			}
			long eTime = System.currentTimeMillis();
			System.out.println("count is : " + count + "; accepted is "
					+ accepted + ";time:" + (eTime - sTime));

		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			this.hbaseUtil.closeTableHandler();
		}
		return results;
	}

	@Override
	public void copQueryPoint(double latitude, double longitude) {
		// TODO Auto-generated method stub

	}

	@Override
	public void scanQueryPoint(double latitude, double longitude) {
		long sTime = System.currentTimeMillis();
		// build up a raster
		XRaster raster = new XRaster(this.space, this.min_size_of_height,
				this.max_num_of_column);
		ResultScanner rScanner = null;

		try {
			// match rect to find the subspace it belongs to
			XBox match_box = raster.locate(latitude, Math.abs(longitude));
			System.out.println("match_box is : " + match_box.toString());
			String[] rowRange = new String[2];
			rowRange[0] = match_box.getRow();
			rowRange[1] = match_box.getRow();

			// the version here is harded coded, because i cannot get how many
			// objects in one cell now
			rScanner = this.hbaseUtil.getResultSet(rowRange, null,
					this.familyName, new String[] { match_box.getColumn() },
					100);
			BixiReader reader = new BixiReader();
			int count = 0;

			for (Result r : rScanner) {
				NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = r
						.getMap();

				for (byte[] family : resultMap.keySet()) {
					NavigableMap<byte[], NavigableMap<Long, byte[]>> columns = resultMap
							.get(family);
					for (byte[] col : columns.keySet()) {
						NavigableMap<Long, byte[]> values = columns.get(col);
						for (Long version : values.keySet()) {
							count++;
							// get the distance between this point and the given
							// point
							XStation station = reader.getStationFromJson(Bytes
									.toString(values.get(version)));
							System.out.println(station.getId());
						}
					}
				}
			}
			long eTime = System.currentTimeMillis();
			System.out.println("count=>" + count + ";time=>"+ (eTime - sTime));

		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			this.hbaseUtil.closeTableHandler();
		}

	}

	@Override
	public void copQueryArea(double latitude, double longitude, int area) {
		// TODO Auto-generated method stub

	}

	@Override
	public void scanQueryArea(double latitude, double longitude, int area) {
		// TODO Auto-generated method stub

	}

	@Override
	public void copQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int n) {
		// TODO Auto-generated method stub

	}

	@Override
	public void scanQueryAvailableKNN(String timestamp, double latitude,
			double longitude, int n) {
		// TODO Auto-generated method stub

	}

}
