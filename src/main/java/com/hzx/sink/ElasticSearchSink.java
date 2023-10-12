package com.hzx.sink;

import com.hzx.avro.AvroBean;
import com.hzx.elastic.JestManager;
import com.hzx.tuple.BaseEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;

public class  ElasticSearchSink extends RichSinkFunction<AvroBean> implements SinkFunction<AvroBean> {
    private BulkSink bulkSink;
    private JestManager jestManager;

    @Override
    public void invoke(AvroBean t, Context context) throws Exception {

        bulkSink.put(t);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Map<String, String> map = parameterTool.toMap();
        jestManager = JestManager.getInstance();
        jestManager.setConfig(map);
        bulkSink = new BulkSink(map);


    }

    @Override
    public void close() throws Exception {
        if (jestManager != null) {
            // 关闭连接
            jestManager.close();

        }

        if(bulkSink != null){
            bulkSink.close();
        }

    }
}
