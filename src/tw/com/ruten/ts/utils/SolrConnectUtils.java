package tw.com.ruten.ts.utils;

/**
 * Created by jia03740 on 2017/6/8.
 */
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.apache.zookeeper.KeeperException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tw.com.ruten.ts.model.GroupTagUpdateModel;

public class SolrConnectUtils {
    public static Logger LOG = LoggerFactory.getLogger(SolrConnectUtils.class);
    private static SolrZkClient zkClient = null;

    public static void sendDeleteToSolr(String zkHost, String collection, List<String> deletes) throws IOException, InterruptedException, KeeperException{
        sendDeleteToSolr(zkHost, collection, deletes, true, null);
    }

    public static void sendDeleteToSolr(String zkHost, String collection, List<String> deletes, boolean commit, Integer shardNum) throws IOException, InterruptedException, KeeperException{
        try{
            String url = null;
            if(shardNum != null){
                url = getTargetShard(zkHost, collection, "leader", shardNum);
            }else{
                url = getRandomShard(zkHost, collection, "leader");
            }

            if(!url.endsWith("/")){
                url += "/";
            }
            url += "update?boost=1.00&overwrite=true&wt=json";
            if(commit){
                url += "&commitWithin=1000";
            }

            sendDeleteToSolr(url, deletes);
        }catch(Exception e){
            LOG.error("delete solr " + deletes.size() +" docs fail, becaues: " + e.toString());
        }
    }

    @SuppressWarnings("unchecked")
    public static void sendDeleteToSolr(String url, List<String> deletes) throws IOException {
        JSONObject deleteOut = new JSONObject();
        deleteOut.put("delete", deletes);

        sendToSolr(url, deleteOut.toJSONString());

        int idx = 0;
        for(int i=0; i<5; i++){
            idx = url.indexOf('/', idx+1);
        }

        LOG.info("delete " + deletes.size() + ", docs to " + url.substring(0, idx));
    }

    public static void sendToSolr(String zkHost, String collection, Queue<JSONObject> jsonOutput) throws IOException, InterruptedException, KeeperException{
        sendToSolr(zkHost, collection, jsonOutput, true, null);
    }

    public static void sendToSolr(String zkHost, String collection, Queue<JSONObject> jsonOutput, boolean commit) throws IOException, InterruptedException, KeeperException{
        sendToSolr(zkHost, collection, jsonOutput, commit, null);
    }

    public static void sendToSolr(String zkHost, String collection, Queue<JSONObject> jsonOutput, boolean commit, Integer shardNum) throws IOException, InterruptedException, KeeperException{
        try{

            String url = null;

            if(shardNum != null){
                url = getTargetShard(zkHost, collection, "leader", shardNum);
            }else{
                url = getRandomShard(zkHost, collection, "leader");
            }

            if(!url.endsWith("/")){
                url += "/";
            }
            url += "update?boost=1.00&overwrite=true&wt=json";
            if(commit){
                url += "&commitWithin=1000";
            }

            sendToSolr( url, jsonOutput);
        }catch(Exception e){
            LOG.error("update solr " + jsonOutput.size() +" docs fail, becaues: " + e.toString());
        }
    }


    @SuppressWarnings("unchecked")
    public static void sendToSolr(String url, Queue<JSONObject> jsonOutput) throws IOException {
        JSONArray out = new JSONArray();
        out.addAll(jsonOutput);

        sendToSolr(url, out.toJSONString());


        int idx = 0;
        for(int i=0; i<5; i++){
            idx = url.indexOf('/', idx+1);
        }

        LOG.info("update " + out.size() + ", docs to " + url.substring(0, idx));
    }

    public static void sendToSolr(String url, String content) throws IOException {
        Response response = Request.Post(url)
                .useExpectContinue()
                .version(HttpVersion.HTTP_1_1)
                .bodyString(content, ContentType.APPLICATION_JSON)
                .execute();

        HttpResponse httpResponse = response.returnResponse();

        if(httpResponse.getStatusLine().getStatusCode() != 200){
            LOG.error("url : " + url);
            LOG.error("http Status: " + httpResponse.getStatusLine().toString());
            StringWriter writer = new StringWriter();
            IOUtils.copy(httpResponse.getEntity().getContent(), writer, "UTF8");
            String responseContent = writer.toString();
            LOG.error("http Content: " + responseContent);
        }
    }

    public static String getJsonFromSolr(String zkHost, String collection, String query) throws IOException{
        try{
            String url = getRandomShard(zkHost, collection, "leader");
            if(!url.endsWith("/")){
                url += "/";
            }
            url += "select?" +query+ "&wt=json";
            return getFromSolr(url);
        }catch(Exception e){
            LOG.error("get from solr (" + query + ") fail, becaues: " + e.toString());
        }
        return null;
    }

    public static String getFromSolr(String url) throws IOException{
        Response response = Request.Get(url)
                .useExpectContinue()
                .version(HttpVersion.HTTP_1_1)
                .execute();

        HttpResponse httpResponse = response.returnResponse();

        if(httpResponse.getStatusLine().getStatusCode() != 200){
            LOG.error("url : " + url);
            LOG.error("http Status: " + httpResponse.getStatusLine().toString());
            StringWriter writer = new StringWriter();
            IOUtils.copy(httpResponse.getEntity().getContent(), writer, "UTF8");
            String responseContent = writer.toString();
            LOG.error("http Content: " + responseContent);
        }


        StringWriter writer = new StringWriter();
        IOUtils.copy(httpResponse.getEntity().getContent(), writer, "UTF8");
        return writer.toString();
    }

    private static String checkForAlias(SolrZkClient zkClient, String collection) throws KeeperException, InterruptedException {
        byte[] aliasData = zkClient.getData(ZkStateReader.ALIASES, null, null, true);
        Aliases aliases = ClusterState.load(aliasData);
        String alias = aliases.getCollectionAlias(collection);

        if (alias != null) {
            List<String> aliasList = StrUtils.splitSmart(alias, ",", true);
            if (aliasList.size() > 1) {
                throw new IllegalArgumentException("collection cannot be an alias that maps to multiple collections");
            }
            collection = aliasList.get(0);
        }

        return collection;
    }

    private static List<Replica> getShards(String zkHost, String collection, String type) throws InterruptedException, KeeperException {
        if (collection == null) {
            throw new IllegalArgumentException("collection must not be null");
        }
        
        if (zkClient == null) {
        	zkClient = new SolrZkClient(zkHost, 1000);
        }
        
        @SuppressWarnings("resource")
        ZkStateReader zkStateReader = new ZkStateReader(zkClient);

        // first check for alias
        collection = checkForAlias(zkClient, collection);
        zkStateReader.createClusterStateWatchersAndUpdate();
        ClusterState state = zkStateReader.getClusterState();
        Set<String> liveNodes = state.getLiveNodes();
        DocCollection docCollection = state.getCollection(collection);
        // check error

        List<Replica> replicas = new ArrayList<Replica>();
        for(String node : liveNodes){
            List<Replica> tmpReplicas = docCollection.getReplicas(node);
            if(tmpReplicas != null){
                replicas.addAll(tmpReplicas);
            }
        }

        Set<Replica> removeSet = new HashSet<Replica>();
        if("leader".equalsIgnoreCase(type)){
            for(Replica replica: replicas){
                if(!replica.containsKey("leader")){
                    removeSet.add(replica);
                }
            }
        }else if("slave".equalsIgnoreCase(type)){
            for(Replica replica: replicas){
                if(replica.containsKey("leader")){
                    removeSet.add(replica);
                }
            }
        }

        if(removeSet.size() > 0){
            replicas.removeAll(removeSet);
        }

        return replicas;
    }


    private static String getRandomShard(String zkHost, String collection, String type) throws InterruptedException, KeeperException {
        List<Replica> replicas = getShards(zkHost, collection, type);
        Random randomGenerator = new Random();
        int index = randomGenerator.nextInt(replicas.size());
        return replicas.get(index).getCoreUrl();
    }

    private static String getTargetShard(String zkHost, String collection, String type, int shardNum) throws InterruptedException, KeeperException {
        List<Replica> replicas = getShards(zkHost, collection, type);
        return replicas.get(shardNum).getCoreUrl();
    }
    
    public static void closeZkClient() {
    	zkClient.close();
    }

    public static void main(String args[]) throws Exception {
        long start = System.currentTimeMillis();
        System.out.println(getRandomShard(args[0], args[1], args[2]));
        System.out.println( "spend: "  + String.valueOf(System.currentTimeMillis() - start )) ;
    }
}