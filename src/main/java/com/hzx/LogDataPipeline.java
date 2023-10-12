package com.hzx;

import com.google.common.collect.Maps;
import com.hzx.avro.AvroBean;
import com.hzx.elastic.JestManager;
import com.hzx.elastic.PutTemplate;
import com.hzx.kafka.KafkaUtils;
import com.hzx.kafka.LogEventKafkaSource;
import com.hzx.tuple.AvroDeserializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;

/**
 *
 * 程序入口 FLink 读取Kafka 默认写入 Elasticsearch
 *
 * 1. 支持自定义解析不同的Kafka的数据格式
 * 2. 支持不同Sink
 */
public class LogDataPipeline {
    private final static Logger log =
            LoggerFactory.getLogger(LogDataPipeline.class);


    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            log.info("需指定配置文件位置");
            System.exit(0);
        }
        ParameterTool parameter = ParameterTool.fromPropertiesFile(args[0]);

        Properties config = parameter.getProperties();
        log.info("配置文件: {}", config);
        final String kafkaTopic = config.getProperty("kafka.topic.name",
                "test");

        log.info("消费的topic为: {}", kafkaTopic);


        // 判断topic是否存在，如果不存在，就创建
        KafkaUtils kafkaUtils = new KafkaUtils(config);

        NewTopic newTopic = new NewTopic(kafkaTopic,
                parseInt(config.getProperty("kafka.topic.partitions", "1")),
                parseShort(config.getProperty("kafka.topic.replicationFactor"
                        , "2")));

        kafkaUtils.ensureTopics(newTopic);

        Properties kafkaConsumerProperties = kafkaUtils.getKafkaConsumerProperties(config);
//        kafkaConsumerProperties.setProperty("flink.partition-discovery.interval-millis", "5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况
        //todo
        String es_template = config.getProperty("es.template");
//        String es_index_prefix = config.getProperty("es.index.prefix");
        String es_index_prefix = config.getProperty("kafka.topic.name");
        if (StringUtils.isNotBlank(es_template)) {
            //todo
            JestManager jestManager = JestManager.getInstance();
            jestManager.setConfig(Maps.fromProperties(config));
//        String test = "{    \"index_patterns\" : [\"h-miguan*\"],    " +
//                "\"settings\": {            \"number_of_shards\": 2,         " +
//                "   \"number_of_replicas\": 0,            \"index.refresh_interval\" : \"10s\"     },     \"mappings\": {             \"properties\": {        \"@timestamp\": {\"type\": \"date\", \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"},  \"ip\": {                    \"type\": \"keyword\"                },            \"time\": {                    \"type\": \"date\",                    \"format\": \"yyyy-MM-dd HH:mm:ss\"},                 \"business_type\": {                    \"type\": \"keyword\"                },                 \"host\": {                    \"type\": \"keyword\"                },                \"offset\": {                    \"type\": \"long\"                },                 \"message\": {                    \"type\": \"text\"                 },                 \"loglevel\":{                   \"type\": \"keyword\"                 } ,                 \"status\":{                   \"type\": \"keyword\"                 }           }     }}";
            PutTemplate putTemplate = new PutTemplate(es_index_prefix,
                    es_template);
            CloseableHttpResponse response = jestManager.execute(putTemplate);
            if (response.getStatusLine().getStatusCode() != 200) {
                log.info("模板更新错误:{}", EntityUtils.toString(response.getEntity()));
                response.close();
            }
            if (response != null && response.getStatusLine().getStatusCode() == 200) {
                log.info("索引: {},模板更新成功", es_index_prefix);
                response.close();
            }
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // env字段如果不为空
        if (!config.getProperty("env", "").equals("test")) {
            long checkpoint_time = Long.parseLong(config.getProperty("flink.checkpoint.interval",
                    "60000"));
            env.enableCheckpointing(checkpoint_time, CheckpointingMode.AT_LEAST_ONCE);
            String checkpoint_path = config.getProperty("fink.checkpoint.path",
                    "hdfs:///tmp/flink/" + kafkaTopic);
            env.setStateBackend(new FsStateBackend(checkpoint_path));
        }
        env.getConfig().setGlobalJobParameters(parameter);

//        env.getConfig().registerTypeWithKryoSerializer(Event.class,
//                ProtobufSerializer.class);
        if (StringUtils.isNotBlank(config.getProperty("parallelism"))) {
            env.setParallelism(parseInt(config.getProperty("parallelism")));
        } else {
            env.setParallelism(1);
        }
       DeserializationSchema  deserializationSchema  = new AvroDeserializationSchema();
        //       String source = config.getProperty("deserialize.class");
//       if(StringUtils.isNotBlank(source)){
//           Class<? extends DeserializationSchema> newInstance =
//                   (Class<? extends DeserializationSchema>) Class.forName(source);
//           deserializationSchema = newInstance.newInstance();
//       }else {
//           deserializationSchema = new LogDeserializationSchema();
//       }
        FlinkKafkaConsumer<List<AvroBean>> kafkaSource =
                new LogEventKafkaSource(kafkaTopic,
                         kafkaConsumerProperties,deserializationSchema);


        DataStream<List<AvroBean>> logEvents = env.addSource(kafkaSource).name("source from kafka");
    
        SingleOutputStreamOperator<AvroBean> avroBeanSingleOutputStreamOperator = logEvents.flatMap(new FlatMapFunction<List<AvroBean>, AvroBean>() {
            @Override
            public void flatMap(List<AvroBean> avroBeans, Collector<AvroBean> collector) throws Exception {
                for (AvroBean avroBean : avroBeans) {
                    collector.collect(avroBean);
                }
            }
        });
    
        DataStream<AvroBean> events =avroBeanSingleOutputStreamOperator;
        String type = config.getProperty("consumer_data.type","");
        ConsumerData consumerData = ConsumerPipelineFactory.getConsumerData(type);
        consumerData.consumerData(events,env,config);
    }
}
