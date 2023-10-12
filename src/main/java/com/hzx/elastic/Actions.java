package com.hzx.elastic;

import com.google.gson.Gson;

public interface Actions {

    public String getURI();

    String getRestMethodName();

    public String getData(Gson gson);
}
