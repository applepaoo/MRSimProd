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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.TsConf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * 過濾 24h、與買廣告的賣家(不列入分群計算)
 * Created by jia03740 on 2017/6/19.
 */
public class ProdClusteringFilterJob extends Configured implements org.apache.hadoop.util.Tool {
    public static Logger LOG = Logger.getLogger(ProdClusteringFilterJob.class);
    public Configuration conf;
    private static String GOODS_FILE_FORMAT = "goods.file.format";
    private static String ASD_WEIGHT_FILE_FORMAT = "asdweight.file.format";
    private static String CLUSTERING_FORMAT = "clustering.file.format";

    public static class BritemorderMapper extends Mapper<Object, MapWritable, Text, MapWritable> {
        @Override
        public void map(Object key, MapWritable value, Context context) throws IOException, InterruptedException {
            MapWritable outValue = new MapWritable();
            outValue.put(new Text("RANK"), value.get(new Text("RANK")));
            outValue.put(new Text("CTRL_ROWID"), value.get(new Text("CTRL_ROWID")));
            context.write(new Text(key.toString()), outValue);
        }
    }

    public static class BuyrankitemMapper extends Mapper<Object, MapWritable, Text, MapWritable> {
        @Override
        public void map(Object key, MapWritable value, Context context) throws IOException, InterruptedException {
        	String bStatus = value.get(new Text("B_STATUS")).toString();
        	String bCoin = value.get(new Text("B_COIN")).toString();
            if (bStatus.equalsIgnoreCase("r") && !"0".equals(bCoin)) {
                MapWritable outValue = new MapWritable();
                outValue.put(new Text("EXCLUDE"), new Text("Y"));
                outValue.put(new Text("CTRL_ROWID"), value.get(new Text("CTRL_ROWID")));
                context.write(new Text(key.toString()), outValue);
            }
        }
    }

    public static class GoodsMapper extends Mapper<Object, MapWritable, Text, MapWritable> {
        private Configuration conf;
        private String[] clusterField;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            clusterField = conf.getStrings(CLUSTERING_FORMAT);
        }

        @Override
        public void map(Object key, MapWritable value, Context context) throws IOException, InterruptedException {
            Text gPriorityOrder = value.get(new Text("G_PRIORITY_ORDER")) instanceof NullWritable ? new Text("") : (Text) value.get(new Text("G_PRIORITY_ORDER"));
//            Text bCoin = value.get(new Text("B_COIN")) instanceof NullWritable ? new Text("") : (Text) value.get(new Text("B_COIN"));
            Text gCloseDate = value.get(new Text("G_CLOSE_DATE")) instanceof NullWritable ? new Text("") : (Text) value.get(new Text("G_CLOSE_DATE"));
            Text isDelete = value.get(new Text("IS_DELETE")) instanceof NullWritable ? new Text("N") : (Text) value.get(new Text("IS_DELETE"));
            if ((gPriorityOrder.toString().isEmpty() || gPriorityOrder.toString().equals("0")) &&
                    gCloseDate.toString().isEmpty() && !isDelete.toString().equalsIgnoreCase("Y")
//                    && (bCoin.toString().isEmpty() || "0".equals(bCoin.toString()))
                    ) { // 廣告品不列入計算
                MapWritable outValue = new MapWritable();
                outValue.put(new Text("INCLUDE"), new Text("Y"));
                for (String keyField : clusterField) {
                    Text keyTextField = new Text(keyField);
                    if (value.containsKey(keyTextField)) {
                        outValue.put(keyTextField, value.get(keyTextField));
                    }
                }
                context.write(new Text(key.toString()), outValue);
            }
        }
    }

    public static class ProdClusteringFilterJobReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        private Configuration conf;
        private List<String> clusterField;
        private Set<Text> ignoreCID = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            clusterField = Arrays.asList(conf.getStrings(CLUSTERING_FORMAT));
            List<String> asdWeightFields = Arrays.asList(conf.getStrings(ASD_WEIGHT_FILE_FORMAT));

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(conf.get("asdweight.file.path")))));
            String str;
            while ((str = br.readLine()) != null) {
                String[] data = str.split("\t");
                if (data.length == asdWeightFields.size()) {
                    if (data[asdWeightFields.indexOf("APPLY_STATUS")].equalsIgnoreCase("A")) {
                        ignoreCID.add(new Text(data[asdWeightFields.indexOf("CTRL_ROWID")]));
                    }
                }
            }

            br.close();
        }

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable tmpValue = new MapWritable();
            for (MapWritable val : values) {
                if (!val.get(new Text("CTRL_ROWID")).toString().equals("(null)")) {
                    Text cid = (Text) val.get(new Text("CTRL_ROWID"));
                    if (!ignoreCID.contains(cid)) {
                        tmpValue.putAll(val);
                    }
                }
            }

            if (!tmpValue.containsKey(new Text("EXCLUDE")) && tmpValue.containsKey(new Text("INCLUDE"))) {
                MapWritable outVal = new MapWritable();
                for (String field : clusterField) {
                    if (tmpValue.containsKey(new Text(field))) {
                        outVal.put(new Text(field), tmpValue.get(new Text(field)));
                    } else {
                        outVal.put(new Text(field), new Text("(null)"));
                    }
                }

                context.write(key, outVal);
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        conf = getConf();

        if (args.length != 5) {
            System.err.println("Usage: ProdClusteringFilterJob <asdweight tsv file path> <buyrankitem seq dir path> <goods seq dir path> <out>");
            return -1;
        }

        conf.set("asdweight.file.path", args[0]);
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[4]);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "ProdClusteringFilterJob");

        job.setJarByClass(ProdClusteringFilterJob.class);
        job.setReducerClass(ProdClusteringFilterJobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, BuyrankitemMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), SequenceFileInputFormat.class, GoodsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), SequenceFileInputFormat.class, BritemorderMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        /// lock file
        return JobUtils.sumbitJob(job, true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(TsConf.create(), new ProdClusteringFilterJob(), args);
        System.exit(res);
    }
}
