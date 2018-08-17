package tw.com.ruten.ts.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import tw.com.ruten.ts.mapreduce.JImageHashClusterTest.SortedKey.GroupComparator;
import tw.com.ruten.ts.mapreduce.JImageHashClusterTest.SortedKey.SortComparator;
import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.TsConf;

public class JImageHashClusterTest extends Configured implements Tool {

	public static Logger LOG = Logger.getLogger(ProdClusteringJob.class);
	public Configuration conf;

	public static class SortedKey implements WritableComparable<SortedKey> {
		Text sortValue = new Text();
		Text defaultKey = new Text();

		SortedKey() {
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			defaultKey.set(Text.readString(in));
			sortValue.set(in.readLine());
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, defaultKey.toString());
			out.writeBytes(sortValue.toString());
		}

		@Override
		public int compareTo(SortedKey other) { /// default
			return this.defaultKey.compareTo(other.defaultKey);
		}

		public int sort(SortedKey other) { /// for sort
			int r = this.defaultKey.compareTo(other.defaultKey);
			if (r == 0) {
				return this.sortValue.toString().compareTo(other.sortValue.toString());
			}

			return r;
		}

		public int group(SortedKey other) { /// for group
			return compareTo(other);
		}

		@Override
		public int hashCode() { /// for partition
			return this.defaultKey.toString().hashCode();
		}

		public static class SortComparator extends WritableComparator {
			SortComparator() {
				super(SortedKey.class, true);
			}

			@Override
			public int compare(WritableComparable o1, WritableComparable o2) {
				if (o1 instanceof SortedKey && o2 instanceof SortedKey) {
					SortedKey k1 = (SortedKey) o1;
					SortedKey k2 = (SortedKey) o2;

					return k1.sort(k2);
				}

				return o1.compareTo(o2);
			}
		}

		public static class GroupComparator extends WritableComparator {
			GroupComparator() {
				super(SortedKey.class, true);
			}

			@Override
			public int compare(WritableComparable o1, WritableComparable o2) {
				if (o1 instanceof SortedKey && o2 instanceof SortedKey) {
					SortedKey k1 = (SortedKey) o1;
					SortedKey k2 = (SortedKey) o2;

					return k1.group(k2);
				}

				return o1.compareTo(o2);
			}
		}
	}

	public static class JImageHashClusterMapper extends Mapper<LongWritable, Text, SortedKey, MapWritable> {
		private Configuration conf;
		private JSONParser parser = new JSONParser();
		String IMG_HASH = null;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			IMG_HASH = conf.get("hashkey", "IMG_HASH_V2");
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String v = value.toString();
			try {

				Object obj = parser.parse(v);
				JSONObject jsonObject = (JSONObject) obj;

				SortedKey outKey = new SortedKey();
				outKey.defaultKey.set(jsonObject.get(IMG_HASH).toString());
				outKey.sortValue.set(jsonObject.get("G_NO").toString());

				MapWritable outValue = new MapWritable();
				// outValue.put(new Text("G_NO"), new Text(jsonObject.get("G_NO").toString()));

				context.write(outKey, outValue);
				context.getCounter("Mapper", "out").increment(1);

			} catch (ParseException e) {
				e.printStackTrace();
				context.getCounter("Mapper", "parse.exception").increment(1);
			}

		}

	}

	public static class JImageHashClusterReducer extends Reducer<SortedKey, MapWritable, Text, Text> {
		private Configuration conf;

		private Text keyText = new Text();
		private Text valueText = new Text();
		public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'");
		int threshold = 0;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			threshold = conf.getInt("threshold", 1);
		}

		public void reduce(SortedKey key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {

			String preHashKey = null;
			String preGno = null;
			boolean cluster = false;
			for (MapWritable val : values) {

				String hashKey = key.defaultKey.toString();

				if (preHashKey == null) {
					preHashKey = hashKey;
					preGno = key.sortValue.toString();
				} else {

					if (hammingDistance(hashKey, preHashKey) < threshold) {

						context.write(new Text(preHashKey), new Text(key.sortValue)); // HASHKEY, GNO
						cluster = true;
					} else {
						preHashKey = hashKey;
						preGno = key.sortValue.toString();
						cluster = false;
					}
				}

			}

			if (cluster == true) {
				context.write(new Text(preHashKey), new Text(preGno));
			}

		}

		static BigInteger hexToBigInt(String s) { // hex to binary
			return new BigInteger(s, 16);
		}

		static int hammingDistance(String i, String i2) { // hamming distance
			return hexToBigInt(i).xor(hexToBigInt(i2)).bitCount();
			// return i.xor(i2).bitCount();
		}

	}

	public int run(String[] args) throws Exception {
		conf = getConf();
		String jobtime = String.valueOf(System.currentTimeMillis());
		
		if (args.length < 2) {
			System.err.println("Usage: JImageHashCluster <input path> <output path>");
			return -1;
		}

		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(args[1],jobtime);
		fs.delete(outputPath, true);

		Job job = Job.getInstance(conf, "JImageHashCluster");

		job.setJarByClass(JImageHashClusterTest.class);
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
		job.setOutputFormatClass(TextOutputFormat.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		MultipleOutputs.addNamedOutput(job, "result", TextOutputFormat.class, NullWritable.class, Text.class);
		job.waitForCompletion(true);
		return JobUtils.sumbitJob(job, true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(TsConf.create(), new JImageHashClusterTest(), args);
		System.exit(res);
	}

}
