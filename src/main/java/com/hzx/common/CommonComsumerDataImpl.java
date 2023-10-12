package com.hzx.common;

import com.hzx.ConsumerData;
import com.hzx.avro.AvroBean;
import com.hzx.sink.ElasticSearchSink;
import com.hzx.tuple.BaseEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CommonComsumerDataImpl implements ConsumerData {
    private final static Logger log =
            LoggerFactory.getLogger(CommonComsumerDataImpl.class);


    @Override
    public void consumerData(DataStream<AvroBean> events, StreamExecutionEnvironment env, Properties config) {
        events.addSink(new ElasticSearchSink()).name("sink to es");
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
