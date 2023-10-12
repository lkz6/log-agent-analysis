package com.hzx.elastic;

import com.google.gson.Gson;

public class PutTemplate implements Actions {
    public String getURI() {
        return buildURI();
    }

    private final String source;

    private final  String templateName;

    private String buildURI() {
        return "_template/" + templateName;
    }

    public String getRestMethodName() {
        return "PUT";
    }


    public PutTemplate(String templateName, String source) {
        this.templateName = templateName;
        this.source = source;
    }

    @Override
    public String getData(Gson gson) {
        return source;
    }
}
