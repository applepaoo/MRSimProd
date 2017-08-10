package tw.com.ruten.ts.mapreduce;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.simple.parser.ParseException;
import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.TsConf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * 計算 Group 相關資訊(GROUP_NUM、GROUP_HEAD)
 * Created by jia03740 on 2017/6/12.
 */
public class ProdClusteringRefineJob extends Configured implements Tool {
    public static Logger LOG = Logger.getLogger(ProdClusteringRefineJob.class);
    public Configuration conf;
    private static String CLUSTERING_FORMAT = "clustering.file.format";
    public static SimpleDateFormat sdfSolr = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'");

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
            try {
                if (r == 0) {
                    String[] data1 = this.sortValue.toString().split("\t");
                    String[] data2 = other.sortValue.toString().split("\t");

                    BigInteger directPrice1 = new BigInteger(data1[0]);
                    BigInteger directPrice2 = new BigInteger(data2[0]);
                    int r1 = directPrice1.compareTo(directPrice2);
                    if (r1 == 0) {
                        long lastUpdate1 = sdfSolr.parse(data1[1]).getTime();
                        long lastUpdate2 = sdfSolr.parse(data2[1]).getTime();

                        if (lastUpdate1 > lastUpdate2) {
                            return -1;
                        } else if (lastUpdate1 < lastUpdate2) {
                            return 1;
                        }else {
                            BigInteger gno1 = new BigInteger(data1[2]);
                            BigInteger gno2 = new BigInteger(data2[2]);

                            return gno2.compareTo(gno1);
                        }

                    }

                    return directPrice1.compareTo(directPrice2);
                }
            }catch (Exception e){
                LOG.error("{}",e);
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

    public static class ProdClusteringRefineJobMapper extends Mapper<Object, Text, SortedKey, MapWritable> {
        private Configuration conf;
        private List<String> clusterField;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            clusterField = Arrays.asList(conf.getStrings(CLUSTERING_FORMAT));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            if (clusterField != null && clusterField.size() == data.length) {
                SortedKey outKey = new SortedKey();
                outKey.defaultKey.set(data[clusterField.indexOf("FINGERPRINT")]);
                outKey.sortValue.set(data[clusterField.indexOf("G_DIRECT_PRICE")] + "\t" + data[clusterField.indexOf("G_LASTUPDATE")] + "\t" + data[clusterField.indexOf("G_NO")]);

                MapWritable outValue = new MapWritable();
                for (String keyField : clusterField) {
                    outValue.put(new Text(keyField), new Text(data[clusterField.indexOf(keyField)]));
                }
                outValue.remove(new Text("GROUP_NUM"));
                context.write(outKey, outValue);
            } else if (data.length == 2) {
                SortedKey outKey = new SortedKey();
                outKey.defaultKey.set(data[0]);
                outKey.sortValue.set("-1");
                MapWritable outValue = new MapWritable();
                outValue.put(new Text("GROUP_NUM"), new Text(data[1]));
                context.write(outKey, outValue);
            }
        }
    }

    public static class ProdClusteringRefineJobReducer extends Reducer<SortedKey, MapWritable, NullWritable, Text> {
        private Configuration conf;
        private List<String> clusterField;
        private Text result = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            clusterField = Arrays.asList(conf.getStrings(CLUSTERING_FORMAT));
        }

        public void reduce(SortedKey key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            String clusterNum = "";
            int i = 0;
            for (MapWritable val : values) {
                MapWritable map = new MapWritable(val);
                if (i == 0) {
                    if(!map.containsKey(new Text("GROUP_NUM"))){
                        break;
                    }

                    clusterNum = map.get(new Text("GROUP_NUM")).toString();
                    i++;
                    continue;
                }

                if (i == 1) {
                    val.put(new Text("GROUP_HEAD"), new Text("Y"));
                } else {
                    val.put(new Text("GROUP_HEAD"), new Text("N"));
                }

                val.put(new Text("GROUP_NUM"), new Text(clusterNum));

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
                context.write(NullWritable.get(), result);
                i++;
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ProdClusteringRefineJob <in> <out>");
            return -1;
        }

        conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path currentPath = new Path(args[1], "current");
        Path oldPath = new Path(args[1], "old");
        fs.delete(oldPath, true);
        if (fs.exists(currentPath)) {
            fs.rename(currentPath, oldPath);
            fs.delete(currentPath, true);
        }

        Job job = Job.getInstance(conf, "ProdClusteringRefineJob");

        job.setJarByClass(ProdClusteringRefineJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ProdClusteringRefineJobMapper.class);

        /// map reduce change
        job.setMapOutputKeyClass(SortedKey.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setReducerClass(ProdClusteringRefineJobReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setSortComparatorClass(SortedKey.SortComparator.class);
        job.setGroupingComparatorClass(SortedKey.GroupComparator.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1], "current"));

        return JobUtils.sumbitJob(job, true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(TsConf.create(), new ProdClusteringRefineJob(), args);
        System.exit(res);
    }
}
