package com.hzx.sink;

import com.hzx.common.AnalysisParser;
import com.hzx.tuple.BaseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BulkSink {
    private final static Logger log =
            LoggerFactory.getLogger(BulkSink.class);
    private final SinkManager sinkManager;

    private  AnalysisParser logAnalysisTools;


    public BulkSink(Map<String, String> map) {
        sinkManager = new SinkManager(map);
        String className = map.getOrDefault("log.parser.class", "com.hzx.common" +
                ".LogAnalysisTools");
        log.info("加载类名: {}",className);
        try {
            Class<? extends AnalysisParser> newInstance =
                    (Class<? extends AnalysisParser>) Class.forName(className);
            logAnalysisTools = newInstance.newInstance();

            logAnalysisTools.setConfig(map);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.info("class not found: {}",className);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public void   put(BaseEvent event) {
        sinkManager.addAction(logAnalysisTools.logParser(event));
    }

    public void close() throws Exception {
        sinkManager.close();
    }


}
