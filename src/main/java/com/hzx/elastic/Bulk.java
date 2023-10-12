package com.hzx.elastic;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Bulk implements Actions {

    private  final static Logger log = LoggerFactory.getLogger(Bulk.class);
    private List<IndexAction> bulkableActions;
    private long lastUpdatime;
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<IndexAction> getbulkableActions(){
        return this.bulkableActions;
    }

    public Bulk() {

//
        bulkableActions =
                new ArrayList<>();
        this.lastUpdatime = System.currentTimeMillis() / 1000;
    }


    public long getLastUpdatime() {
        return this.lastUpdatime;
    }

    @Override
    public String getData(Gson gson) {
        /*
        { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
        { "field1" : "value1" }
        { "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
         */
        StringBuilder sb = new StringBuilder();
        for (IndexAction action : bulkableActions) {
            // e.g.: { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
            Map<String, Map<String, String>> opMap = new LinkedHashMap<String, Map<String, String>>();

            Map<String, String> opDetails =
                    new LinkedHashMap<String, String>();
            if (StringUtils.isNotBlank(action.getId())) {
                opDetails.put("_id", action.getId());
            }
            if (StringUtils.isNotBlank(action.getIndexName())) {
                opDetails.put("_index", action.getIndexName());
            }
            opDetails.put("_type", "_doc");

            opMap.put("index", opDetails);
            sb.append(gson.toJson(opMap));
            sb.append("\n");

            // write out the action source/document line
            // e.g.: { "field1" : "value1" }
            Object source = getIndexData(gson, action.getPayload());
            if (source != null) {
                sb.append(getJson(gson, source));
                sb.append("\n");
            }

        }
        return sb.toString();
    }


    public String getIndexData(Gson gson, Object payload) {
        if (payload == null) {
            return null;
        } else if (payload instanceof String) {
            return (String) payload;
        } else {
            return gson.toJson(payload);
        }
    }

    private Object getJson(Gson gson, Object source) {
        if (source instanceof String) {
            return source;
        } else {
            return gson.toJson(source);
        }
    }






    public Integer getActionSize() {
        return this.bulkableActions.size();
    }

    public void clear() {
         this.bulkableActions.clear();
    }
    @Override
    public String getURI() {
        return buildURI();
    }

    private String buildURI() {
        return "/_bulk";
    }

    @Override
    public String getRestMethodName() {
        return "POST";
    }
}
