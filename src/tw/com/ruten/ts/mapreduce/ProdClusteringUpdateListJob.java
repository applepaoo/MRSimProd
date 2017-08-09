package tw.com.ruten.ts.mapreduce;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import tw.com.ruten.ts.model.GroupTagUpdateModel;
import tw.com.ruten.ts.utils.JobUtils;
import tw.com.ruten.ts.utils.SolrConnectUtils;
import tw.com.ruten.ts.utils.TsConf;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;




/**
 * Created by jia03740 on 2017/6/5.
 */
public class ProdClusteringUpdateListJob extends Configured implements Tool {
    public final static int MULTI_NUM = 3;
    private static String CLUSTERING_FORMAT = "clustering.file.format";
    public static Logger LOG = Logger.getLogger(ProdClusteringUpdateListJob.class);
    public Configuration conf;

    public static int getPartition(Text key) {
        int hash = org.apache.solr.common.util.Hash.murmurhash3_x86_32(key.toString(), 0, key.toString().length(), 0);

        return getPartition(hash);
    }

    public static int getPartition(int hash) {
        if (hash <= 0x9554ffff) {
            return 0;
        } else if (hash <= 0xaaa9ffff) {
            return 1;
        } else if (hash <= 0xbfffffff) {
            return 2;
        } else if (hash <= 0xd554ffff) {
            return 3;
        } else if (hash <= 0xeaa9ffff) {
            return 4;
        } else if (hash <= 0xffffffff) {
            return 5;
        } else if (hash <= 0x1554ffff) {
            return 6;
        } else if (hash <= 0x2aa9ffff) {
            return 7;
        } else if (hash <= 0x3fffffff) {
            return 8;
        } else if (hash <= 0x5554ffff) {
            return 9;
        } else if (hash <= 0x6aa9ffff) {
            return 10;
        } else {
            return 11;
        }
    }

    public static class SolrShardPartitioner extends Partitioner<Text, MapWritable> {
        @Override
        public int getPartition(Text key, MapWritable value, int reduceNum) {
            int hash = org.apache.solr.common.util.Hash.murmurhash3_x86_32(key.toString(), 0, key.toString().length(), 0);
            return ProdClusteringUpdateListJob.getPartition(hash)+((hash&0x7FFFFFFF)%3)*12;
        }
    }

    public static class ProdClusteringUpdateListJobMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Configuration conf;
        private Text outKey = new Text();
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
                MapWritable outValue = new MapWritable();
                outKey.set(data[clusterField.indexOf("G_NO")]);
                outValue.put(new Text("FINGERPRINT"), new Text(data[clusterField.indexOf("FINGERPRINT")]));
                outValue.put(new Text("G_LASTUPDATE"), new Text(data[clusterField.indexOf("G_LASTUPDATE")]));
                outValue.put(new Text("G_CLOSE_DATE"), new Text(data[clusterField.indexOf("G_CLOSE_DATE")]));
                outValue.put(new Text("GROUP_HEAD"), new Text(data[clusterField.indexOf("GROUP_HEAD")]));
                outValue.put(new Text("GROUP_NUM"), new Text(data[clusterField.indexOf("GROUP_NUM")]));
                outValue.put(new Text("UPDATE"), new Text(data[clusterField.indexOf("UPDATE")]));
                context.write(outKey, outValue);
            }
        }
    }


    public static class ProdClusteringUpdateListJobReducer extends Reducer<Text, MapWritable, Text, Text> {
        private Configuration conf;
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'");
        private Comparator<MapWritable> selfComparator = new Comparator<MapWritable>() {
            @Override
            public int compare(MapWritable o1, MapWritable o2) {
                Text o1Time = (Text) o1.get(new Text("UPDATE"));
                Text o2Time = (Text) o2.get(new Text("UPDATE"));
                try {
                    long t1 = sdf.parse(o1Time.toString()).getTime();
                    long t2 = sdf.parse(o2Time.toString()).getTime();
                    if (t1 > t2) {
                        return -1;
                    } else if (t1 < t2) {
                        return 1;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        };
        private Text result = new Text();
        private Gson gson = new GsonBuilder().serializeNulls().create();
        private Queue<JSONObject> groupTagUpdateModels = new LinkedList<>();
        private JSONParser jsonParser = new JSONParser();
        private int insertKey = -1;
        private boolean isWrite2Solr = false;

        protected void setup(Context context) {
            conf = context.getConfiguration();
            isWrite2Solr = context.getConfiguration().getBoolean("isWrite2Solr", false);
        }

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            if(insertKey == -1){
                insertKey = getPartition(key);
            }

            List<MapWritable> list = new LinkedList<>();
            for (MapWritable val : values) {
                list.add(new MapWritable(val));
            }

            Collections.sort(list, selfComparator);
            MapWritable current = list.get(0);
            Text currentGCloseDate = (Text) current.get(new Text("G_CLOSE_DATE"));
            Text currentFingerprint = (Text) current.get(new Text("FINGERPRINT"));
            boolean current_group_head = ((Text) current.get(new Text("GROUP_HEAD"))).toString().equals("Y") ? true : false;
            int current_group_num = Integer.parseInt(((Text) current.get(new Text("GROUP_NUM"))).toString());

            try {
                if (list.size() > 1) {
                    MapWritable last = list.get(1);
                    Text lastGCloseDate = (Text) last.get(new Text("G_CLOSE_DATE"));
                    Text lastFingerprint = (Text) last.get(new Text("FINGERPRINT"));
                    int last_group_num = Integer.parseInt(((Text) last.get(new Text("GROUP_NUM"))).toString());
                    boolean last_group_head = ((Text) last.get(new Text("GROUP_HEAD"))).toString().equals("Y") ? true : false;

                    if ((lastGCloseDate.toString().isEmpty() && last_group_num > 1) && (!currentGCloseDate.toString().isEmpty() || current_group_num <= 1)) { // 變下架或集群變1
                        result.set(currentFingerprint.toString() + "\t" + current_group_head + "\t" + current_group_num + "\t" + currentGCloseDate.toString() + "\tdelete");
                        if(isWrite2Solr) {
                            groupTagUpdateModels.add((JSONObject) jsonParser.parse(gson.toJson(new GroupTagUpdateModel(key.toString(), null, null, null))));
                        }
                    } else if ((!lastGCloseDate.toString().isEmpty() || last_group_num <= 1) && (currentGCloseDate.toString().isEmpty() && current_group_num > 1)) { // 變上架且集群大於1
                        result.set(currentFingerprint.toString() + "\t" + current_group_head + "\t" + current_group_num + "\t" + currentGCloseDate.toString()  + "\tupdate");
                        if(isWrite2Solr) {
                            groupTagUpdateModels.add((JSONObject) jsonParser.parse(gson.toJson(new GroupTagUpdateModel(key.toString(), currentFingerprint.toString(), current_group_head, current_group_num))));
                        }
                    } else if (currentGCloseDate.toString().isEmpty() && current_group_num > 1 &&
                            (!lastFingerprint.toString().equalsIgnoreCase(currentFingerprint.toString()) || (current_group_head ^ last_group_head))) { //上架中商品 & 集群大於1 & (群集更動 | head 更動)
                        result.set(currentFingerprint.toString() + "\t" + current_group_head + "\t" + current_group_num + "\t" + currentGCloseDate.toString()  + "\tupdate");
                        if(isWrite2Solr) {
                            groupTagUpdateModels.add((JSONObject) jsonParser.parse(gson.toJson(new GroupTagUpdateModel(key.toString(), currentFingerprint.toString(), current_group_head, current_group_num))));
                        }
                    }
                } else {
                    if (currentGCloseDate.toString().isEmpty() && current_group_num > 1) { //上架中 & 集群大於0
                        result.set(currentFingerprint.toString() + "\t" + current_group_head + "\t" + current_group_num + "\t" + currentGCloseDate.toString()  + "\tinsert");
                        if(isWrite2Solr) {
                            groupTagUpdateModels.add((JSONObject) jsonParser.parse(gson.toJson(new GroupTagUpdateModel(key.toString(), currentFingerprint.toString(), current_group_head, current_group_num))));
                        }
                    }
                }
            }catch (org.json.simple.parser.ParseException e){
                LOG.error(e.toString());
            }
            if (result.toString().length() > 0) {

                if (isWrite2Solr && groupTagUpdateModels.size() >= 1000) { /// flush
                    try {
                        SolrConnectUtils.sendToSolr("172.25.8.223:2181,172.25.8.224:2181,172.25.8.225:2181", "product1", groupTagUpdateModels, false);
                        groupTagUpdateModels = new LinkedList<>();
                    } catch (Exception e) {
                        context.write(key, new Text(e.toString()));
                    }

                    Thread.sleep(100);
                }
                context.write(key, result);
                result.clear();
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException{
            try {
                if(isWrite2Solr && groupTagUpdateModels.size() > 0){
                    SolrConnectUtils.sendToSolr("172.25.8.223:2181,172.25.8.224:2181,172.25.8.225:2181", "product1", groupTagUpdateModels, false);
                    groupTagUpdateModels = new LinkedList<>();
                }
            } catch (Exception e) {
                context.write(new Text("final"), new Text(e.toString()));
            }
            SolrConnectUtils.closeZkClient();
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        conf = getConf();
        if (args.length < 2) {
            System.err.println("Usage: ProdClusteringUpdateListJob <current cluster file path> <output file path> <isWrite2Solr>");
            return -1;
        }

        if(args.length > 2){
            conf.setBoolean("isWrite2Solr", Boolean.parseBoolean(args[2]));
        }

        String currentFilePath = args[0];
        Path outputFilePath = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputFilePath, true);
        Job job = Job.getInstance(conf, "ProdClusteringUpdateListJob");


        job.setJarByClass(ProdClusteringUpdateListJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ProdClusteringUpdateListJobMapper.class);
//        job.setPartitionerClass(SolrShardPartitioner.class);
        job.setReducerClass(ProdClusteringUpdateListJobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setNumReduceTasks(12 * MULTI_NUM);
//        job.setNumReduceTasks(37);

        FileInputFormat.addInputPaths(job, currentFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);


        return JobUtils.sumbitJob(job, true, args[0].split(",")) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(TsConf.create(), new ProdClusteringUpdateListJob(), args);
        System.exit(res);
    }
}
