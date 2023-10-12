package com.hzx.avro;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Main {
 //   private static final String KAFKA_TOPIC = "agent-test1";
 //   private static final String KAFKA_BOOTSTRAP_SERVERS = "20.224.42.181:9092,20.224.42.182:9092";


    public static void main(String[] args) {
        String topic = args[0];
        String kafkaServer = args[1];
    
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Consumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));
            records.forEach(record -> {
                byte[] value  = record.value();
                try {
                    List<AvroBean> avroBeans = AvroUtils.doSerialize(value, topic);
                    System.out.println("解析大小: " + avroBeans.size());
                    for (AvroBean avroBean : avroBeans) {
                        System.out.println("message: " + avroBean.getMessage() + ", offset: " + avroBean.getOffset() + ", timestamp: " + avroBean.getTimestamp() + ", topic: " + avroBean.getTopic());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }


}
