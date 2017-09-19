package tw.com.ruten.ts.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.ruten.tool.SimHash;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tw.com.ruten.ts.mapreduce.ProdClusteringJob.SortedKey.GroupComparator;
import tw.com.ruten.ts.mapreduce.ProdClusteringJob.SortedKey.SortComparator;
import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.TsConf;

/**
 * 同一賣家內的商品進行分群
 */
public class ProdClusteringJob extends Configured implements Tool {
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

    public static class ProdClusteringJobMapper extends Mapper<Object, MapWritable, SortedKey, MapWritable> {
        private Configuration conf;
        private SimHash simHash = new SimHash(Hashing.murmur3_128());
        private HashFunction hf = Hashing.sha1();
        private HashSet<String> stopwordList = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            for (String word : conf.getStrings(STOPWORD_FILE)) {
                stopwordList.add(word);
            }
        }

        @Override
        public void map(Object key, MapWritable value, Context context) throws IOException, InterruptedException {
                SortedKey outKey = new SortedKey();
                outKey.defaultKey.set(value.get(new Text("CTRL_ROWID")).toString());
                String fingerprint = value.get(new Text("CTRL_ROWID")).toString() + "," + value.get(new Text("G_NAME")).toString();
                outKey.sortValue.set(hf.hashString(fingerprint, Charset.defaultCharset()).toString());
                context.write(outKey, value);
        }
    }

    public static class ProdClusteringJobReducer extends Reducer<SortedKey, MapWritable, NullWritable, Text> {
        public static SimpleDateFormat sdfSolr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'");
        private Configuration conf;
        private MultipleOutputs mos;
        public static String THRESHOLD_SIMILARITY = "threshold.similarity";
        private double threshold;
        private Text info = new Text();
        private Text result = new Text();
        private Text clusterInfo = new Text();
        private HashFunction hf = Hashing.sha1();
        private List<String> clusterField;

        @Override
        public void setup(Reducer.Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            threshold = context.getConfiguration().getDouble(THRESHOLD_SIMILARITY, 0.1d);
            clusterField = Arrays.asList(conf.getStrings(CLUSTERING_FORMAT));
            mos = new MultipleOutputs(context);
        }

        public void reduce(SortedKey key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            int prodCount = 0, clusterCount = 0, clusterNum = 0;
            String last = null;

            for (MapWritable val : values) {
                val.put(new Text("UPDATE"), new Text(sdfSolr.format(new Date())));
                String current = key.sortValue.toString();
                val.put(new Text("FINGERPRINT"), new Text(current));

                if (last != null) { // 第一個商品之後接要與先前的商品比較是否一樣
                    if (!current.equalsIgnoreCase(last)) {
                        clusterInfo.set(last + "\t" + clusterNum);// 儲存上一個集群的資訊
                        mos.write("clusterInfo", NullWritable.get(), clusterInfo, "clusterInfo/part");
                        clusterNum = 0;

                        clusterCount++;
                    }
                }else{// 第一個商品，直接計算Fingerprint
                    clusterCount++;
                }


                prodCount++;
                clusterNum++;
                last = current;

                String outValue = "";
                for (String keyField : clusterField) {
                    Text tmp = new Text(keyField);
                    if (!outValue.isEmpty()) {
                        outValue += "\t";
                    }
                    if (val.containsKey(tmp)) {
                        Writable writable = val.get(tmp);
                        outValue += writable.toString();
                    } else {
                        outValue += "(null)";
                    }
                }

                result.set(outValue);
                mos.write("result", NullWritable.get(), result, "result/part");
            }
            clusterInfo.set(last + "\t" + clusterNum); // 儲存最後集群的資訊
            mos.write("clusterInfo", NullWritable.get(), clusterInfo, "clusterInfo/part");
            info.set(new Text(key.defaultKey + "\t" + (clusterCount == 0 && prodCount == 0 ? 0.0 : (clusterCount / (float) prodCount))) + "\t" + clusterCount + "\t" + prodCount);
            mos.write("info", NullWritable.get(), info, "info/part");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        conf = getConf();

        if (args.length < 2) {
            System.err.println("Usage: ProdClusteringJob <in> <out>");
            return -1;
        }
        if (args.length > 2) {
            conf.setDouble(ProdClusteringJobReducer.THRESHOLD_SIMILARITY, Double.parseDouble(args[2]));
        }

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "ProdClusteringJob");

        job.setJarByClass(ProdClusteringJob.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(ProdClusteringJobMapper.class);

        /// map reduce change
        job.setMapOutputKeyClass(SortedKey.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setReducerClass(ProdClusteringJobReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setSortComparatorClass(SortComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        MultipleOutputs.addNamedOutput(job, "info", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "clusterInfo", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "result", TextOutputFormat.class, NullWritable.class, Text.class);

        return JobUtils.sumbitJob(job, true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(TsConf.create(), new ProdClusteringJob(), args);
        System.exit(res);
    }
}
