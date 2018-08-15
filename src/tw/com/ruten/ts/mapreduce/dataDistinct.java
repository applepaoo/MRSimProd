package tw.com.ruten.ts.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.TsConf;

public class dataDistinct extends Configured implements Tool {

	public static Logger LOG = Logger.getLogger(ProdClusteringJob.class);
	private static String CLUSTERING_FORMAT = "clustering.file.format";
	private static String STOPWORD_FILE = "stopword.file";
	public Configuration conf;

	public static class dataDistinctMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Configuration conf;
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] test = value.toString().split("\\t");

			context.write(new Text(test[0]), one);

		}

	}

	public static class dataDistinctReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Configuration conf;
		private IntWritable result = new IntWritable();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();

		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
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

		Job job = Job.getInstance(conf, "dataDistinct");

		job.setJarByClass(dataDistinct.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setJobName("dataDistinct");

		// mapper
		job.setMapperClass(dataDistinctMapper.class);
		// job.setMapOutputKeyClass(Text.class);
		job.setCombinerClass(dataDistinctReducer.class);
		job.setReducerClass(dataDistinctReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
		return JobUtils.sumbitJob(job, true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(TsConf.create(), new dataDistinct(), args);
		System.exit(res);
	}

}
