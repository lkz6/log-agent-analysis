package com.hzx.elastic;

import com.google.gson.Gson;

public class ClusterHealth implements Actions{
    public enum Status {
        RED("red"), YELLOW("yellow"), GREEN("green");

        private final String key;

        Status(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }

    public enum Level {
        CLUSTER("cluster"), INDICES("indices"), SHARDS("shards");

        private final String key;

        Level(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }

    protected ClusterHealth() {

    }


    protected String buildURI() {
        StringBuilder sb = new StringBuilder("/_cluster/health/");


        return sb.toString();
    }

    @Override
    public String getURI() {
        return buildURI();
    }

    @Override
    public String getRestMethodName() {
        return "GET";
    }

    @Override
    public String getData(Gson gson) {
        return null;
    }


}
