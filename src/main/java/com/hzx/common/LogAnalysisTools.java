package com.hzx.common;

import com.google.common.base.CharMatcher;
import com.hzx.avro.AvroBean;
import com.hzx.elastic.IndexAction;

import com.hzx.grok.dictionary.GrokDictionary;
import com.hzx.grok.util.Grok;
import com.hzx.sink.ElasticSearchUtils;
import com.hzx.tuple.BaseEvent;

import com.hzx.tuple.Event;
import com.hzx.util.DateUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LogAnalysisTools implements AnalysisParser {
    
    private final static String GROKER_NAME = "grok.message.pattern";
    private final static Logger log =
            LoggerFactory.getLogger(LogAnalysisTools.class);
    

    private String datePattern;
    
    
    private String children;
    
    @Override
    public void setConfig(Map<String, String> config) {
        
    
        this.datePattern = config.getOrDefault("es.index.date.pattern",
                "yyyy.MM.dd");
        this.children = config.getOrDefault("es.index.contain.name",
                "false");
        log.info("索引时间格式为: {}", datePattern);
    }
    
    @Override
    public IndexAction logParser(BaseEvent event) {
    
        IndexAction indexAction = new IndexAction();
        AvroBean avroBean = (AvroBean) event;
        Map<String, Object> map = new HashMap<>();
        String topic = avroBean.getTopic();
        String name = avroBean.getName();
        String message = avroBean.getMessage();
        long time = avroBean.getTimestamp();
        long offset = avroBean.getOffset();
        String ip = avroBean.getIp();
        String timestamp = DateUtils.getLogTime(time);
        



        if(children.equals("false")){
            indexAction.setIndexName(topic +
                                             "-" + ElasticSearchUtils.getIndexName(timestamp, datePattern));
        }else {
            if (topic.equals(name) || StringUtils.isBlank(name)) {
                indexAction.setIndexName(topic +
                                                 "-" + ElasticSearchUtils.getIndexName(timestamp, datePattern));//timestamp_add_8hours  ->timestamp
            } else {
                indexAction.setIndexName(topic +
                                                 "-" + name + "-" + ElasticSearchUtils.getIndexName(timestamp, datePattern));//timestamp_add_8hours  ->timestamp
            }
        }
        /*
        message: 2023-10-08 11:11:06,151 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Null container completed..., offset: 4720936, timestamp: 1697011576999, topic: 1697011576999
         */
    
        try {
            if (message.contains("|")){
                String[] lines = message.split("\\|");
                String line = lines[0];
                String event_time = DateUtils.timeZoneConvert(line,DatePattern.getPatternbyDateStr(line));
                map.put("event_time",event_time);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
       
    
        map.put("message",message);
        map.put("timestamp",timestamp);
        map.put("offset",offset);
        map.put("ip",ip);
        map.put("name",name);
        map.put("flink_handle_time", DateUtils.getNowDay1());
 //       map.put("topic",topic);

    
//        indexAction.setIndexName("555555");
        indexAction.setPayload(map);
    
//        log.info("数据:{}",indexAction.getPayload().toString());
        return indexAction;
    }
    
    
}
