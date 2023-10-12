package com.hzx.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaUtils {
    private static final String kafka_consumer_prefix = "kafka.consumer.";
    private static final String kafka_producer_prefix = "kafka.consumer.";

    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

    private AdminClient adminClient;
    private Properties adminConfigs;

    public KafkaUtils(Properties properties) {
        adminConfigs = getKafkaConsumerProperties(properties);
    }


    public void ensureTopics(NewTopic targetTopic) {

        try (AdminClient adminClient = getAdminClient()) {
            Set<String> topics = adminClient.listTopics().names().get(10,
                    TimeUnit.SECONDS);
            List<NewTopic> requiredTopics =
                    Collections.singletonList(targetTopic);
            for (NewTopic requiredTopic : requiredTopics) {
                if (!topics.contains(requiredTopic.name())) {
                    getAdminClient().createTopics(Collections.singletonList(requiredTopic)).all().get();
                    log.info("Topic {} create success.", requiredTopic.name());
                } else {
                    log.info("Topic {} already exists.", requiredTopic.name());
                }
            }
        } catch (Exception e) {
            log.error("Error ensuring topics are created", e);
        }
    }

    private AdminClient getAdminClient() {
        if (adminClient == null) {
            synchronized (this) {
                if (adminClient == null) {
                    adminClient = AdminClient.create(adminConfigs);
                }
            }
        }
        return adminClient;
    }


    public Properties getKafkaConsumerProperties(Properties config) {
        Properties props = new Properties();
        for (Enumeration<?> e = config.propertyNames(); e.hasMoreElements(); ) {
            String name = (String) e.nextElement();
            String value = config.getProperty(name);
            if (name.startsWith(kafka_consumer_prefix)) {
                props.put(name.split(kafka_consumer_prefix)[1], value);
            }
        }

        String groupid = config.getProperty("group.id");
        if(StringUtils.isBlank(groupid)){

            props.setProperty("group.id", config.getProperty("kafka.topic.name"));
        }else {
            props.setProperty("group.id", groupid);

        }

        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        return props;
    }




    public Properties getProducerProperties(Properties config) {
        Properties props = new Properties();
        for (Enumeration<?> e = config.propertyNames(); e.hasMoreElements(); ) {
            String name = (String) e.nextElement();
            String value = config.getProperty(name);
            if (name.startsWith(kafka_producer_prefix)) {
                props.put(name.split(kafka_producer_prefix)[1], value);
            }
        }

        String groupid = config.getProperty("group.id");
        if(StringUtils.isBlank(groupid)){

            props.setProperty("group.id", config.getProperty("kafka.topic.name"));
        }else {
            props.setProperty("group.id", groupid);

        }

        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        return props;
    }

    public Properties getProducerProperties(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "ipaddress:6667");
        properties.put("acks"             , "0");
        properties.put("retries"          , "1");
        properties.put("batch.size"       , "20971520");
        properties.put("linger.ms"        , "33");
        properties.put("max.request.size" , "2097152");
        properties.put("compression.type" , "gzip");
        properties.put("key.serializer"   , "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("kafka.topic"      , "my-topic");
        return properties;
    }
}
