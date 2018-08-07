package tw.com.ruten.ts.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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

public class JImageHashCluster extends Configured implements Tool {

	public static Logger LOG = Logger.getLogger(ProdClusteringJob.class);
	private static String CLUSTERING_FORMAT = "clustering.file.format";
	private static String STOPWORD_FILE = "stopword.file";
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

	public static class JImageHashClusterMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
		private Configuration conf;
		private JSONParser parser = new JSONParser();
		private Text outKey = new Text();
		private Set<String> removeFields = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String v = value.toString();
			try {

				Object obj = parser.parse(v);
				JSONObject jsonObject = (JSONObject) obj;

				outKey.set(jsonObject.get("IMG_HASH_V1").toString());

				MapWritable outValue = new MapWritable();

				outValue.put(new Text("G_NO"), new Text(jsonObject.get("G_NO").toString()));
				// Set<String> keySet = jsonObject.keySet();
				//
				// for (String k : keySet) {
				//
				// Object obj1 = jsonObject.get(k);
				// outValue.put(new Text(k), new Text((String) obj1));
				//
				// }

				context.write(outKey, outValue);
				context.getCounter("Mapper", "out").increment(1);

			} catch (ParseException e) {
				e.printStackTrace();
				context.getCounter("Mapper", "parse.exception").increment(1);
			}

		}

	}

	public static class JImageHashClusterReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
		private Configuration conf;
		private MultipleOutputs mos;
		public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'");
		private List<String> clusterField;
		private Text outKey = new Text();
		private Text outValue = new Text();
		private Text info = new Text();
		private Text result = new Text();
		private Text clusterInfo = new Text();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			clusterField = Arrays.asList(conf.getStrings(CLUSTERING_FORMAT));
			mos = new MultipleOutputs(context);
		}

		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			for (MapWritable value : values) {
				context.write(key, value);
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// mos.close();
		}
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

		job.setJarByClass(JImageHashCluster.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setJobName("JImageHash Clustering");

		// mapper
		job.setMapperClass(JImageHashClusterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		// reducer
		job.setReducerClass(JImageHashClusterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// job.setSortComparatorClass(SortComparator.class);
		// job.setGroupingComparatorClass(GroupComparator.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		MultipleOutputs.addNamedOutput(job, "result", TextOutputFormat.class, NullWritable.class, Text.class);
		job.waitForCompletion(true);
		return JobUtils.sumbitJob(job, true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(TsConf.create(), new JImageHashCluster(), args);
		System.exit(res);
	}

}
