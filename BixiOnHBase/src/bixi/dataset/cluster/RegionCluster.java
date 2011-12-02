package bixi.dataset.cluster;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/*
 * Region cluster class
 * cluster the stations according to its physical location
 */
public class RegionCluster
{
	
	private static String srcTable = "rowData";
	private static String dstTable = "clusterID";
	

	private static int latitudeSplitNum = 4;
	private static int langitudeSplitNum = 4;

	
	//non-arg constructor
	public RegionCluster(String srcTable, String dstTable, int latitudeSplitNum, int langitudeSplitNum)
	{
		this.dstTable = dstTable;
		this.srcTable = srcTable;
		
		this.latitudeSplitNum = latitudeSplitNum;
		this.langitudeSplitNum = langitudeSplitNum;
	}
	
	//create and configure a new mapreduce job
	public static Job jobConf(Configuration config) throws IOException, InterruptedException, ClassNotFoundException
	{	
		Job job = new Job(config,"station cluster");
		
		//make it rowSplitNum * columnSplitNum
		job.setNumReduceTasks(latitudeSplitNum * langitudeSplitNum);
			        
		Scan scan = new Scan();
		//make it 10
		scan.setCaching(10);
		//I dont know why should I set it to false
		scan.setCacheBlocks(false);
			        
		//initiate the map job
		TableMapReduceUtil.initTableMapperJob
		(
			// input table
			srcTable,
			// Scan instance to control CF and attribute selection
			scan,               
			// mapper class
			ClusterMapper.class,     
			// mapper output key
			Text.class,
			// mapper output value
			Text.class,  
			job
		);
		
		TableMapReduceUtil.initTableReducerJob
		(
			// output table
			dstTable,        
			// reducer class
			ClusterReducer.class,    
			job
		);
		
		/*
		//dummy data
		//for test use
		ArrayList<Double> latitudeSplitLocation = new ArrayList<Double>(Arrays.<Double>asList(4.0, 8.0, 12.0, 16.0, 20.0));
		ArrayList<Double> longitudeSplitLocation = new ArrayList<Double>(Arrays.<Double>asList(3.0, 6.0, 9.0, 12.0, 15.0));
		
		ArrayList<Double> latitudeSplitLength = new ArrayList<Double>(Arrays.<Double>asList(4.0, 4.0, 4.0, 4.0));
		ArrayList<Double> longitudeSplitLength = new ArrayList<Double>(Arrays.<Double>asList(3.0, 3.0, 3.0, 3.0));
		*/
		
		
		return job;
			    
	}
	
	//divide the region according to the station distribution
	public static void divideRegion
	(
			int numStations,
			//latitude-wise station distribution info
			int numLatitudeSplit, ArrayList<Integer> latitudeDistribution, 
			//longitude-wise station distribution info
			int numLongitudeSplit, ArrayList<Integer> longitudeDistribution,
			
			double latitudeLow, double latitudeHigh,
			double longitudeLow, double longitudeHigh
	)
	{
		
		//resultant division method
		//latitude-wise breakpoint location and longitude-wise breakpoint location
		ArrayList<Double> latitudeSplitLocation = new ArrayList<Double>();
		ArrayList<Double> longitudeSplitLocation = new ArrayList<Double>();
		
		//length between each breakpoint
		ArrayList<Double> latitudeSplitLength = new ArrayList<Double>();
		ArrayList<Double> longitudeSplitLength = new ArrayList<Double>();
		
		
		//========================================== divide the region latitude-wisely
		double numStationsPerBucket = (double)numStations/(double)numLatitudeSplit;
		int numStationsUntilNow = 0;
		
		double currentPos = latitudeLow;
		double previousPos = latitudeLow;
		
		latitudeSplitLocation.add(latitudeLow);
		
		for (int i = 1; i < latitudeDistribution.size(); i++)
		{
			numStationsUntilNow += latitudeDistribution.get(i-1);
			
			if (numStationsUntilNow >= numStationsPerBucket)
			{
				currentPos = latitudeLow + i * ((latitudeHigh - latitudeLow) / latitudeDistribution.size());
				
				latitudeSplitLocation.add(currentPos);
				latitudeSplitLength.add(currentPos - previousPos);
				
				numStationsUntilNow = 0;
				
				previousPos = currentPos;
			}
		}
		
		currentPos = latitudeHigh;
		latitudeSplitLocation.add(currentPos);
		latitudeSplitLength.add(currentPos - previousPos);
		
		//========================================== divide the region longitude-wisely
		numStationsPerBucket = (double)numStations/(double)numLongitudeSplit;
		numStationsUntilNow = 0;
		
		currentPos = longitudeLow;
		previousPos = longitudeLow;
		
		longitudeSplitLocation.add(longitudeLow);
		
		for (int i = 1; i < longitudeDistribution.size(); i++)
		{
			numStationsUntilNow += longitudeDistribution.get(i-1);
			
			if (numStationsUntilNow >= numStationsPerBucket)
			{
				currentPos = longitudeLow + i * ((longitudeHigh - longitudeLow) / longitudeDistribution.size());
				
				longitudeSplitLocation.add(currentPos);
				longitudeSplitLength.add(currentPos - previousPos);
				
				numStationsUntilNow = 0;
				
				previousPos = currentPos;
			}
		}
		
		currentPos = longitudeHigh;
		longitudeSplitLocation.add(currentPos);
		longitudeSplitLength.add(currentPos - previousPos);
		
		//for test use
		/*
		System.out.println("latitude split location: ");
		for (double latitude: latitudeSplitLocation)
		{
			System.out.println(latitude + " ");
		}
		System.out.println();
		
		System.out.println("latitude split length: ");
		for (double latitudeLength: latitudeSplitLength)
		{
			System.out.println(latitudeLength + " ");
		}
		System.out.println();
		
		
		System.out.println("longitude split location: ");
		for (double longitude: longitudeSplitLocation)
		{
			System.out.println(longitude + " ");
		}
		System.out.println();
		
		System.out.println("longitude split length: ");
		for (double longitudeLength: longitudeSplitLength)
		{
			System.out.println(longitudeLength + " ");
		}
		System.out.println();
		*/
		
		
		//initiate the mapper
		//send split information to mapper so that it knows how to cluster the data points
		ClusterMapper.setSplitInfo(latitudeSplitLocation, longitudeSplitLocation, latitudeSplitLength, longitudeSplitLength);
		
	}
	
	public static void main(String[] args) throws Exception 
	{
		//must call this function first to get the station distribution information
		DistributionInfo.computDistributionInfo(srcTable, 4, 5);
		
		//now we have latitude-wisely and longitude-wisely distribution information
		ArrayList<Integer> latitudeDistribution = DistributionInfo.getLatitudeDistribution();
		ArrayList<Integer> longitudeDistribution = DistributionInfo.getLongitudeDistribution();
		
		//also we can get the smallest latitude value and highest latitude value
		double latitudeLow = DistributionInfo.getLatitudeLow();
		double latitudeHigh = DistributionInfo.getLatitudeHigh();
		
		//so is the longitude value
		double longitudeLow = DistributionInfo.getLongitudeLow();
		double longitudeHigh = DistributionInfo.getLongitudeHigh();
		
		//we can also get the total number of stations
		int numStations = DistributionInfo.getNumStations();
		
		//for test use
		/*
		System.out.println("latitude distribution: ");
		for (int latitudeBucket: latitudeDistribution)
		{
			System.out.print(latitudeBucket + ": ");
		}
		System.out.println();
		
		System.out.println("longitude distribution: ");
		for (int longitudeBucket: longitudeDistribution)
		{
			System.out.print(longitudeBucket + ": ");
		}
		System.out.println();
		*/
		
		//for test use
		/*
		ArrayList<Integer> latitudeDistribution = new ArrayList<Integer>(Arrays.<Integer>asList(3, 7, 1, 1, 5, 3));
		ArrayList<Integer> longitudeDistribution = new ArrayList<Integer>(Arrays.<Integer>asList(4, 4, 4, 4, 4));
		*/
		
		//divide the whole region into pieces, in which each sub-region has roughly the same amount of stations
		divideRegion(numStations, 4, latitudeDistribution, 5, longitudeDistribution, latitudeLow, latitudeHigh, longitudeLow, longitudeHigh);
		
		
		//now we can go on to the mapreduce job to cluster the stations
		Configuration conf = HBaseConfiguration.create();
		
		Job job = jobConf(conf);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}

/*
 * the mapper class that clusters all the stations
 */
class ClusterMapper extends TableMapper<Text, Text>
{
		private static ArrayList<Double> latitudeSplitLocation  = new ArrayList<Double>();
		private static ArrayList<Double> longitudeSplitLocation = new ArrayList<Double>();
		
		private static ArrayList<Double> latitudeSplitLength = new ArrayList<Double>();
		private static ArrayList<Double> longitudeSplitLength = new ArrayList<Double>();
		
		//deep copy
		//we need to possess the so-called split information
		//etc. how many buckets we want to have if we divide the region latitude-wisely
		public static void setSplitInfo 
		(
				ArrayList<Double> latitudeSplitLocation, 
				ArrayList<Double> langitudeSplitLocation, 
				ArrayList<Double> latitudeSplitLength, 
				ArrayList<Double> langitudeSplitLength
		)
		{
			
			for (double latitude: latitudeSplitLocation)
			{
				ClusterMapper.latitudeSplitLocation.add(new Double(latitude));
			}
			
			for (double langitude: langitudeSplitLocation)
			{
				ClusterMapper.longitudeSplitLocation.add(new Double(langitude));
			}
			
			for (double latitudeLength: latitudeSplitLength)
			{
				ClusterMapper.latitudeSplitLength.add(new Double(latitudeLength));
			}
			
			for (double langitudeLength: langitudeSplitLength)
			{
				ClusterMapper.longitudeSplitLength.add(new Double(langitudeLength));
			}
		}
	
		//the map function
		@Override
   		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException 
   		{
   			//String sid = new String(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("sid")));
   			String sid = new String(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("sid")));
			
   			double latitude = Double.parseDouble(Bytes.toString(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("latitude"))));
   			double longitude = Double.parseDouble(Bytes.toString(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("longitude"))));
   			
   			/*
   			byte[] bbb= value.getValue(Bytes.toBytes("station"), Bytes.toBytes("latitude"));
   			
   			double latitude = Double.parseDouble(Bytes.toString(bbb));
   			bbb= value.getValue(Bytes.toBytes("station"), Bytes.toBytes("longitude"));
   			double longitude = Double.parseDouble(Bytes.toString(bbb));
   			
//   			double latitude = Bytes.toDouble(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("latitude")));
//   			double longitude = Bytes.toDouble(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("longitude")));
			System.out.printf("%s\t%s\t%s\n",sid, latitude, longitude);
			*/
   			
			String cid = getClusterId(sid, latitude, longitude);
   			
   			context.write(new Text(cid), new Text(sid));
   		}
		
		//given the latitude and longitude, this function finds the cluster id for the station
		private String getClusterId(String sid, double latitude, double longitude)
		{
			int i = 0;
			//int j = latitudeSplitLocation.size() - 1;
			
			int pos = 0;
			
			String cid;
			
			boolean set_flag = false;
	
			//=================search in latitude=====================
			for (i = 0; i < latitudeSplitLocation.size() - 1; i++)
			{
				if (latitude == latitudeSplitLocation.get(i))
				{
					pos = i;
					
					set_flag = true;
					break;
				}
				else if (latitudeSplitLocation.get(i) > latitude)
				{
					pos = i - 1;
					
					set_flag = true;
					break;
				}
			}
			
			if ((i == (latitudeSplitLocation.size() - 1)) && (set_flag == false))
			{
				pos = (latitudeSplitLocation.size() - 2);
			}
			
			/*
			double lowBarrier = latitudeSplitLocation.get(i);
			double highBarrier = latitudeSplitLocation.get(j);
			double median = latitudeSplitLocation.get((i+j)/2);
			
			while ((median != latitude) && (i < j - 1))
			{
				if (median < latitude)
				{
					i = (i+j)/2;
				}
				else if (median > latitude)
				{
					j = (i+j)/2;
				}
				
				lowBarrier = longitudeSplitLocation.get(i);
				highBarrier = longitudeSplitLocation.get(j);
				median = longitudeSplitLocation.get((i+j)/2);
			}
			
			if (median == latitude)
			{
				pos = (i+j)/2;
			}
				else if (i <= j)
				{
					pos = i;
					
					if (i == latitudeSplitLocation.size() - 1)
					{
						pos = i - 1;
					}
				}
				else if (i > j)
				{
					pos = i - 1;
				}
			
			if (pos < 0)
			{
				pos = 0;
			}
			else if (pos >= latitudeSplitLocation.size() - 1)
			{
				pos = latitudeSplitLocation.size() - 2;
			}
			*/
			
			//latitude
			cid = Double.toString(latitudeSplitLocation.get(pos));
			cid += new String(":");
			//length
			cid += Double.toString(latitudeSplitLength.get(pos));
			cid += new String(":");
			
			
			//=================binary search in longitude=====================
			i = 0;
			//j = longitudeSplitLocation.size() - 1;
			
			pos = longitudeSplitLocation.size() - 1;
			set_flag = false;
			
			for (i = longitudeSplitLocation.size() - 1; i > 0; i--)
			{
				if (longitude == longitudeSplitLocation.get(i))
				{
					pos = i;
					
					set_flag = true;
					break;
				}
				else if (longitudeSplitLocation.get(i) < longitude)
				{
					pos = i + 1;
					
					set_flag = true;
					break;
				}
			}
			
			if ((i == 0) && (set_flag == false))
			{
				pos = 1;
			}
			
			/*
			lowBarrier = longitudeSplitLocation.get(i);
			highBarrier = longitudeSplitLocation.get(j);
			median = longitudeSplitLocation.get((i+j)/2);
			
			while ((median != longitude) && (i < j - 1))
			{
				if (median < longitude)
				{
					i = (i+j)/2;
				}
				else if (median > longitude)
				{
					j = (i+j)/2;
				}
				
				lowBarrier = longitudeSplitLocation.get(i);
				highBarrier = longitudeSplitLocation.get(j);
				median = longitudeSplitLocation.get((i+j)/2);
			}
			
			if (median == longitude)
			{
				pos = (i+j)/2;
			}
				else if (i <= j)
				{
					pos = j;
					
					if (j == 0)
					{
						pos = 1;
					}
				}
				else if (i > j)
				{
					pos = j+1;
				}
			
			if (pos < 1)
			{
				pos = 1;
			}
			else if (pos > longitudeSplitLocation.size() - 1)
			{
				pos = longitudeSplitLocation.size() - 1;
			}
			*/
			
			//longitude
			cid += Double.toString(longitudeSplitLocation.get(pos));
			cid += new String(":");
			//height
			cid += Double.toString(longitudeSplitLength.get(pos-1));
			
			
			return cid;
		}
}

/*
 * the reducer class that clusters all the stations
 */
class ClusterReducer extends TableReducer<Text, Text, Text>
{
	//actual reduce function
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		//for test use
		/*
		System.out.println("key: " + key);
		
		System.out.print("cid: ");
		
		for (Text cid: values)
		{
			System.out.print(cid + " ");
		}
		
		System.out.println();
		*/
		
		
		String cidSet = new String("");
		for (Text cid: values)
		{
			cidSet += cid.toString();
		}
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("cid"), Bytes.toBytes(cidSet));
		put.add(Bytes.toBytes("station"), Bytes.toBytes("sid"), Bytes.toBytes(key.toString()));

		context.write(null, put);
		
	}
}

/*
 * class that samples the stations distribution
 * we are expected to see some buckets like: [3][4][3][2]. Each bucket records the number
 * of stations that reside in that bucket. In this way, we sample the station distribution
 */
class DistributionInfo
{
	//source table from which we read in the station location information
	private static String srcTable = "rowData";	

	//how many sub-regions we want to divide the original
	//region into
	private static int latitudeSplitNum = 4;
	private static int longitudeSplitNum = 4;
	
	//smallest and largest latitude value
	private static double latitudeLow = 0;
	private static double latitudeHigh = 0;
	
	//smallest and largest longitude value
	private static double longitudeLow = 0;
	private static double longitudeHigh = 0;
	
	private static ArrayList<Integer> latitudeDistribution = null;
	private static ArrayList<Integer> longitudeDistribution = null;
	
	private static int numStations = 0;

	//a bunch of service functions
	public static double getLatitudeLow()
	{
		return DistributionInfo.latitudeLow;
	}
	
	public static double getLatitudeHigh()
	{
		return DistributionInfo.latitudeHigh;
	}
	
	public static double getLongitudeLow()
	{
		return DistributionInfo.longitudeLow;
	}
	
	public static double getLongitudeHigh()
	{
		return DistributionInfo.longitudeHigh;
	}
	
	public static int getLatitudeSplitNum()
	{
		return DistributionInfo.latitudeSplitNum;
	}
	
	public static int getLongitudeSplitNum()
	{
		return DistributionInfo.longitudeSplitNum;
	}
	
	public static void incrementLatitudeBucket(int pos)
	{
		if ((pos < 0) || (pos > DistributionInfo.latitudeDistribution.size() - 1))
		{
			return;
		}
		
		int count = DistributionInfo.latitudeDistribution.get(pos);
		count++;
		
		DistributionInfo.latitudeDistribution.set(pos, count);
	}
	
	public static void incrementLongitudeBucket(int pos)
	{
		if ((pos < 0) || (pos > DistributionInfo.longitudeDistribution.size()))
		{
			return;
		}
		
		int count = longitudeDistribution.get(pos);
		count++;
		
		longitudeDistribution.set(pos, count);
	}
	
	public static final ArrayList<Integer> getLatitudeDistribution()
	{
		return DistributionInfo.latitudeDistribution;
	}
	
	public static final ArrayList<Integer> getLongitudeDistribution()
	{
		return DistributionInfo.longitudeDistribution;
	}
	
	public static void incrementNumStations()
	{
		numStations++;
	}
	
	public static int getNumStations()
	{
		return DistributionInfo.numStations;
	}
	
	//main computation function
	//we perform two rounds' map operations here
	public static void computDistributionInfo
	(
			String srcTable, int latitudeSplitNum, int longitudeSplitNum
	) throws IOException, InterruptedException, ClassNotFoundException
	{
		DistributionInfo.srcTable = srcTable;
		
		DistributionInfo.latitudeSplitNum = latitudeSplitNum;
		DistributionInfo.longitudeSplitNum = longitudeSplitNum;
		
	
		//first round map, used to find out the boundary information
		//like the smallest latitude value and the total number of stations
		Configuration getBoundaryConf = HBaseConfiguration.create();
		
		Job getBoundaryJob = getBoundaryJobConf(getBoundaryConf);
		
		getBoundaryJob.waitForCompletion(true);
		
		
		//second round map, used to model the stations distribution
		Configuration getDistributionConf = HBaseConfiguration.create();
		
		Job getDistributionJob = getDistributionJobConf(getDistributionConf);
		
		getDistributionJob.waitForCompletion(true);
	}
	
	//create and configure a map job for finding out the boundary-information
	public static Job getBoundaryJobConf(Configuration config) throws IOException, InterruptedException, ClassNotFoundException
	{	
		Job job = new Job(config,"get latitude and longitude boundary");
		
		//make it rowSplitNum * columnSplitNum
		//job.setNumReduceTasks(getBoundaryJobLoad);
			        
		Scan scan = new Scan();
		//make it 10
		scan.setCaching(10);
		//I dont know why should I set it to false
		scan.setCacheBlocks(false);
			        
		//initiate the map job
		TableMapReduceUtil.initTableMapperJob
		(
			// input table
			srcTable,
			// Scan instance to control CF and attribute selection
			scan,               
			// mapper class
			GetBoundaryMapper.class,
			// no reducer -> no mapper output key
			null,
			// no reducer -> no mapper output value
			null, 
			job
		);
		
		//no reducer this time
		job.setOutputFormatClass(NullOutputFormat.class);
		
		//it's almost impossible to find a latitude value that is smaller than latitudeHigh, etc.
		latitudeLow = 1000000000;
		latitudeHigh = -1000000000;
		
		longitudeLow = 1000000000;
		longitudeHigh = -1000000000;
		
		numStations = 0;
		
		return job;
	}

	public static void compareLatitude(double latitude)
	{
		if (latitude < DistributionInfo.latitudeLow)
		{
			DistributionInfo.latitudeLow = latitude;
		}
		else if (latitude > DistributionInfo.latitudeHigh)
		{
			DistributionInfo.latitudeHigh = latitude;
		}
	}
	
	public static void compareLongitude(double longitude)
	{
		if (longitude < DistributionInfo.longitudeLow)
		{
			DistributionInfo.longitudeLow = longitude;
		}
		else if (longitude > DistributionInfo.longitudeHigh)
		{
			DistributionInfo.longitudeHigh = longitude;
		}
	}
	
	//create and confiture a map job for sampling the stations distribution
	public static Job getDistributionJobConf(Configuration config) throws IOException, InterruptedException, ClassNotFoundException
	{	
		Job job = new Job(config,"get the station distribution info");
		
		//make it rowSplitNum * columnSplitNum
		//job.setNumReduceTasks(getBoundaryJobLoad);
			        
		Scan scan = new Scan();
		//make it 10
		scan.setCaching(10);
		//I dont know why should I set it to false
		scan.setCacheBlocks(false);
			        
		//initiate the map job
		TableMapReduceUtil.initTableMapperJob
		(
			// input table
			srcTable,
			// Scan instance to control CF and attribute selection
			scan,               
			// mapper class
			GetDistributionMapper.class,
			// no reducer -> no mapper output key
			null,
			// no reducer -> no mapper output value
			null, 
			job
		);
		
		//no reducer this time
		job.setOutputFormatClass(NullOutputFormat.class);
		
		
		latitudeDistribution = new ArrayList<Integer>();
		longitudeDistribution = new ArrayList<Integer>();
		
		for (int i = 0; i < latitudeSplitNum; i++)
		{
			latitudeDistribution.add((int)0);
		}
		
		for (int i = 0; i < longitudeSplitNum; i++)
		{
			longitudeDistribution.add((int)0);
		}
		
		return job;
	}
}

//actual map function of first round
class GetBoundaryMapper extends TableMapper<IntWritable, MapWritable>
{	
	@Override
	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException 
	{
		
		double latitude = Double.parseDouble(Bytes.toString(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("latitude"))));
		double longitude = Double.parseDouble(Bytes.toString(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("longitude"))));
			
		DistributionInfo.compareLatitude(latitude);
		DistributionInfo.compareLongitude(longitude);
		
		DistributionInfo.incrementNumStations();
	}
}

//actual map function of second round
class GetDistributionMapper extends TableMapper<IntWritable, MapWritable>
{	
	@Override
	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException 
	{
		
		double latitude = Double.parseDouble(Bytes.toString(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("latitude"))));
		double longitude = Double.parseDouble(Bytes.toString(value.getValue(Bytes.toBytes("station"), Bytes.toBytes("longitude"))));
			
		
		double latitudeBucketLength = ((DistributionInfo.getLatitudeHigh() - DistributionInfo.getLatitudeLow()) / DistributionInfo.getLatitudeSplitNum());
		
		int latitudePos = (int)((latitude - DistributionInfo.getLatitudeLow())/latitudeBucketLength);
		
		latitudePos = ((latitudePos < DistributionInfo.getLatitudeSplitNum()) ? latitudePos : (DistributionInfo.getLatitudeSplitNum() - 1));
		
		DistributionInfo.incrementLatitudeBucket(latitudePos);
		
		
		
		double longitudeBucketLength = ((DistributionInfo.getLongitudeHigh() - DistributionInfo.getLongitudeLow()) / DistributionInfo.getLongitudeSplitNum());
		
		int longitudePos = (int)((longitude - DistributionInfo.getLongitudeLow())/longitudeBucketLength);
		
		longitudePos = ((longitudePos < DistributionInfo.getLongitudeSplitNum()) ? longitudePos : (DistributionInfo.getLongitudeSplitNum() - 1));
		
		DistributionInfo.incrementLongitudeBucket(longitudePos);
	}
}