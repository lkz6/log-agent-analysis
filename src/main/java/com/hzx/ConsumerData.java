package com.hzx;

import com.hzx.avro.AvroBean;
import com.hzx.tuple.BaseEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public interface ConsumerData {

    void consumerData(DataStream<AvroBean> events, StreamExecutionEnvironment env, Properties config);
}

