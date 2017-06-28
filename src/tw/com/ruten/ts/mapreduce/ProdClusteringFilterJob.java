package tw.com.ruten.ts.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.TsConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by jia03740 on 2017/6/19.
 */
public class ProdClusteringFilterJob extends Configured implements org.apache.hadoop.util.Tool  {
    public static Logger LOG = Logger.getLogger(ProdClusteringFilterJob.class);
    public Configuration conf;
    private static String GOODS_FILE_FORMAT = "goods.file.format";
    private static String ASD_WEIGHT_FILE_FORMAT = "asdweight.file.format";

//    public static class AsdweightMapper  extends Mapper<Object, MapWritable, Text, MapWritable> {
//        @Override
//        public void map(Object key, MapWritable value, Context context) throws IOException, InterruptedException {
//            if(value.get(new Text("APPLY_STATUS")).toString().equalsIgnoreCase("A")){
//                MapWritable outValue = new MapWritable();
//                outValue.put(new Text("ISDELETE"), new Text("Y"));
//                context.write(new Text(key.toString()), outValue);
//            }
//        }
//    }

    public static class BuyrankitemMapper  extends Mapper<Object, MapWritable, Text, MapWritable> {
        @Override
        public void map(Object key, MapWritable value, Context context) throws IOException, InterruptedException {
            if(value.get(new Text("B_STATUS")).toString().equalsIgnoreCase("r")){
                MapWritable outValue = new MapWritable();
                outValue.put(new Text("EXCLUDE"), new Text("Y"));
                outValue.put(new Text("CTRL_ROWID"), value.get(new Text("CTRL_ROWID")));
                context.write(new Text(key.toString()), outValue);
            }
        }
    }

    public static class GoodsMapper  extends Mapper<Object, MapWritable, Text, MapWritable> {
        @Override
        public void map(Object key, MapWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), value);
        }
    }

    public static class ProdClusteringFilterJobReducer extends Reducer<Text,MapWritable,NullWritable,Text> {
        private Configuration conf;
        private Text result = new Text();
        private List<String> goodsFields;
        private Set<Text> ignoreCID = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            goodsFields = Arrays.asList(conf.getStrings(GOODS_FILE_FORMAT));
            List<String> asdWeightFields = Arrays.asList(conf.getStrings(ASD_WEIGHT_FILE_FORMAT));

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(conf.get("asdweight.file.path")))));
            String str;
            while ((str = br.readLine()) != null) {
                String[] data = str.split("\t");
                if(data.length == asdWeightFields.size()){
                    if(data[asdWeightFields.indexOf("APPLY_STATUS")].equalsIgnoreCase("A")){
                        ignoreCID.add(new Text(data[asdWeightFields.indexOf("CTRL_ROWID")]));
                    }
                }
            }

            br.close();
        }

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
                MapWritable tmpValue = new MapWritable();
                for (MapWritable val : values) {
                    Text cid = (Text) val.get(new Text("CTRL_ROWID"));
                    if (!ignoreCID.contains(cid)) {
                        tmpValue.putAll(val);
                    }
                }

                if (!tmpValue.containsKey(new Text("EXCLUDE"))) {
                    StringBuilder builder = new StringBuilder();
                    for (String field : goodsFields) {
                        if (builder.toString().length() != 0) {
                            builder.append("\t");
                        }
                        if (tmpValue.containsKey(new Text(field))) {
                            Writable writable = tmpValue.get(new Text(field));
                            builder.append(writable == null || writable.toString().equals("(null)")  ? "" : writable.toString());
                        } else {
                            builder.append("");
                        }
                    }

                    result.set(builder.toString());
                    context.write(NullWritable.get(), result);
                }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        conf = getConf();

        if (args.length != 4) {
            System.err.println("Usage: ProdClusteringFilterJob <asdweight tsv file path> <buyrankitem seq dir path> <goods seq dir path> <out>");
            return -1;
        }

        conf.set("asdweight.file.path", args[0]);
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[3]);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "ProdClusteringFilterJob");

        job.setJarByClass(ProdClusteringFilterJob.class);
        job.setReducerClass(ProdClusteringFilterJobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, AsdweightMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, BuyrankitemMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), SequenceFileInputFormat.class, GoodsMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        /// lock file
        return JobUtils.sumbitJob(job, true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(TsConf.create(), new ProdClusteringFilterJob(), args);
        System.exit(res);
    }
}
