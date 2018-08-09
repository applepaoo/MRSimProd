package tw.com.ruten.ts.mapreduce;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.TsConf;


/**
 * 
 * cluster sample
 * 
 * input text json input
 * output text 
 * 
 * @author realmeat
 *
 */
public class JImageHashClusterSample extends Configured implements Tool {

	public static Logger LOG = Logger.getLogger(ProdClusteringJob.class);
	private static String CLUSTERING_FORMAT = "clustering.file.format";
	public Configuration conf;

	//public static Text HASH_KEY;
	public static Text G_NO = new Text("G_NO");
	
	public static class JImageHashClusterMapper extends Mapper<LongWritable, Text, SortedKey, MapWritable> {
		private Configuration conf;
		private JSONParser parser = new JSONParser();
		// private Text outKey = new Text();
		String hashKey = null;
		int bitCount = 0;
		int bitShift = 0;
		BigInteger mask;
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			hashKey = conf.get("cluster.hashkey","IMG_HASH_V1");
			bitCount = conf.getInt("cluster.bitcount", 256);
			bitShift = conf.getInt("cluster.bitshift", 64);
			
			StringBuilder strMask = new StringBuilder();
			
			for(int i=0; i<bitShift; i++) {
				strMask.append('1');
			}
			
			mask = new BigInteger(strMask.toString(),2);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String v = value.toString();
			try {

				Object obj = parser.parse(v);
				JSONObject jsonObject = (JSONObject) obj;

				MapWritable outValue = new MapWritable();
				Set<Object> keySet = jsonObject.keySet();
				for(Object tmpKey: keySet) {
					Object tmpValue = jsonObject.get(tmpKey);
					
					outValue.put(new Text(tmpKey.toString()), new Text(tmpValue.toString()));
				}

				// outValue.put(new Text("G_NO"), new Text(jsonObject.get("G_NO").toString()));

				String hash = jsonObject.get(hashKey).toString();
				BigInteger tmp = hex2BigInt(hash);
				
				List<BigInteger> list = new ArrayList<BigInteger>();
				for(int i=0; i<bitCount; i+=bitShift) {
					BigInteger t = tmp.shiftRight(i); 
					list.add(t.and(mask));
				}
				
				for(BigInteger bigInt: list) { /// spread
					String hex = bigInt2Hex(bigInt);
					SortedKey outKey = new SortedKey();
					outKey.defaultKey.set(hex);
					outKey.sortValue.set(hash);
					context.write(outKey, outValue);
				}
				
				context.getCounter("Mapper", "out").increment(1);

			} catch (ParseException e) {
				e.printStackTrace();
				context.getCounter("Mapper", "parse.exception").increment(1);
			}
		}
		
	}

	public static class JImageHashClusterReducer extends Reducer<SortedKey, MapWritable, Text, Text> {
		private Configuration conf;
		int threshold = 0;
		Text targetHash = new Text();	
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			threshold = conf.getInt("cluster.threshold", 128);
			targetHash.set(conf.get("cluster.hashkey","IMG_HASH_V1"));
		}

		public void reduce(SortedKey key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {

			//newkey.set(hexToBin(String.valueOf(key.defaultKey)));
			String preHashKey = null;
			String preGno = null;
			boolean hasSim = false;
			for (MapWritable val : values) {
				String hashKey = val.get(targetHash).toString();
				String gno = val.get(G_NO).toString();
						
				
				if(preHashKey == null) {
					preHashKey = hashKey;
					preGno = gno;
				}else {
					if(dis(hashKey,preHashKey) < threshold) {
						context.write(new Text(gno), new Text(preHashKey));
					}else {
						if(hasSim) {
							context.write(new Text(preGno), new Text(preHashKey));
						}
						
						preHashKey = hashKey;
						preGno = gno;
						hasSim = false;
					}
				}
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		}
	}
	
	static BigInteger hex2BigInt(String in) { 
		return new BigInteger(in, 16);
	}

	static String bigInt2Hex(BigInteger in) {
		return in.toString(16);
	
	}
	static int dis(BigInteger in1, BigInteger in2) { // hamming distance
		return in1.xor(in2).bitCount();
	}
	
	static int dis(String in1, String in2) {
		return dis(hex2BigInt(in1), hex2BigInt(in2));
	}

	public int run(String[] args) throws Exception {
		conf = getConf();

		if (args.length < 2) {
			System.err.println("Usage: JImageHashCluster <input path> <output path>");
			return -1;
		}

		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(args[1]);
		fs.delete(outputPath, true);

		Job job = Job.getInstance(conf, "JImageHashCluster");

		job.setJarByClass(JImageHashClusterSample.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setJobName("JImageHash Clustering");

		// mapper
		job.setMapperClass(JImageHashClusterMapper.class);
		// job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(SortedKey.class);
		job.setMapOutputValueClass(MapWritable.class);

		// reducer
		job.setReducerClass(JImageHashClusterReducer.class);
		job.setMapOutputKeyClass(SortedKey.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setOutputValueClass(MapWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setSortComparatorClass(SortedKey.SortComparator.class);
		job.setGroupingComparatorClass(SortedKey.GroupComparator.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		return JobUtils.sumbitJob(job, true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(TsConf.create(), new JImageHashClusterSample(), args);
		System.exit(res);
	}

}
