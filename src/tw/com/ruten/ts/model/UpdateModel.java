package tw.com.ruten.ts.model;

import com.google.gson.JsonNull;
import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jia03740 on 2017/6/13.
 */
public class UpdateModel {
    private String id;
    @SerializedName("GROUP_TAG")
    private Map<String,String> gTag = new HashMap<>();

    public UpdateModel(String id, String group_tag){
        this.id = id;
        this.gTag.put("set", group_tag);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
