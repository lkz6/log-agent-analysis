package com.hzx.common;

import com.hzx.elastic.IndexAction;
import com.hzx.tuple.BaseEvent;

import java.util.Map;

public interface AnalysisParser {

    void setConfig(Map<String, String> config);
    IndexAction logParser(BaseEvent event);

}
