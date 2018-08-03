package tw.com.ruten.ts.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import tw.com.ruten.ts.mapreduce.ProdClusteringJob.SortedKey;
import tw.com.ruten.ts.mapreduce.ProdClusteringJob.SortedKey.GroupComparator;
import tw.com.ruten.ts.mapreduce.ProdClusteringJob.SortedKey.SortComparator;

public class JImageHashCluster {

	public static Logger LOG = Logger.getLogger(ProdClusteringJob.class);
	private static String CLUSTERING_FORMAT = "clustering.file.format";
	private static String STOPWORD_FILE = "stopword.file";
	public Configuration conf;

	// SortedKey 公用寫法?
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

	public static class JImageHashClusterMapper extends Mapper<Object, MapWritable, SortedKey, MapWritable> {
		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			conf = context.getConfiguration();

		}

		@Override
		public void map(Object key, MapWritable value, Context context) throws IOException, InterruptedException {

			// MAP TODO

		}

	}

	public static class JImageHashClusterReducer extends Reducer<SortedKey, MapWritable, NullWritable, Text> {
		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			conf = context.getConfiguration();

		}

		public void reduce(SortedKey key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {

			// REDUCE TODO
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: JImageHashCluster <input path> <output path>");
		}

		Job job = new Job();
		job.setJarByClass(JImageHashCluster.class);
		job.setJobName("JImageHash Clustering");

		// mapper
		job.setMapperClass(JImageHashClusterMapper.class);
		job.setMapOutputKeyClass(SortedKey.class);
		job.setMapOutputValueClass(MapWritable.class);

		// reducer
		job.setReducerClass(JImageHashClusterReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
