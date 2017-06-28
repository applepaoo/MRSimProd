package tw.com.ruten.ts.model;

/**
 * Created by jia03740 on 2017/6/8.
 */

import com.google.gson.JsonNull;
import com.google.gson.annotations.SerializedName;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * {
 "id":"21608865035093",
 "GROUP_TAG":{"set":null}
 }
 */
public class GroupTagUpdateModel {
    private String id;
    @SerializedName("GROUP_TAG")
    private Map<String,String> gTag = new HashMap<>();

    @SerializedName("GROUP_HEAD")
    private Map<String,Object> gHead = new HashMap<>();

    @SerializedName("GROUP_NUM")
    private Map<String,Object> gNum = new HashMap<>();

    @SerializedName("_IMPORT")
    private Map<String,String> _import = new HashMap<>();

    @SerializedName("_SOURCE_TIME")
    private Map<String,String> sourceTime = new HashMap<>();

    private transient SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHHmmss");
    public  transient SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public GroupTagUpdateModel(String id, String group_tag, Boolean grepu_head, Integer group_num){
        Date date = new Date();
        this.id = id;
        this.gTag.put("set", group_tag);
        this.gHead.put("set", grepu_head);
        this.gNum.put("set", group_num);
        this._import.put("add", "group_tag." + sdf1.format(date));
        this.sourceTime.put("add", sdf2.format(date));
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setgTag(String gTag){
        this.gTag.put("set", gTag);
    }

    public Map<String, String> getgTag() {
        return gTag;
    }
}
