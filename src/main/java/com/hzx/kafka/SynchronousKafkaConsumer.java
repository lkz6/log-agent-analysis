package com.hzx.kafka;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Queue;

public class SynchronousKafkaConsumer<T> extends FlinkKafkaConsumer<T> {
    protected static final Logger LOG = LoggerFactory.getLogger(SynchronousKafkaConsumer.class);

    private  float topicRateLimit;
    private transient RateLimiter subtaskRateLimiter;

    public SynchronousKafkaConsumer(String topic, Properties props, DeserializationSchema<T> valueDeserializer) {
        super(topic, valueDeserializer, props);
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Properties properties = parameterTool.getProperties();
        topicRateLimit = Integer.parseInt(properties.getProperty("topicRateLimit","1"));
        Preconditions.checkArgument(
                topicRateLimit / getRuntimeContext().getNumberOfParallelSubtasks() > 0.1,
                "subtask ratelimit should be greater than 0.1 QPS");
        LOG.info("限流算子：{},task:{}",subtaskRateLimiter,getRuntimeContext().getNumberOfParallelSubtasks());
        subtaskRateLimiter = RateLimiter.create(
                topicRateLimit / getRuntimeContext().getNumberOfParallelSubtasks());
        super.open(configuration);
    }


    @Override
    protected AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {

        // make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
        // this overwrites whatever setting the user configured in the properties
        adjustAutoCommitConfig(properties, offsetCommitMode);

        return new KafkaFetcher<T>(
                sourceContext,
                assignedPartitionsWithInitialOffsets,
                watermarkStrategy,
                runtimeContext.getProcessingTimeService(),
                runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                runtimeContext.getUserCodeClassLoader(),
                runtimeContext.getTaskNameWithSubtasks(),
                deserializer,
                properties,
                pollTimeout,
                runtimeContext.getMetricGroup(),
                consumerMetricGroup,
                useMetrics){
            @Override
            protected void emitRecordsWithTimestamps(Queue<T> records, KafkaTopicPartitionState<T, TopicPartition> partitionState, long offset, long kafkaEventTimestamp) {
                synchronized (checkpointLock) {
                    T record;
                    while ((record = records.poll()) != null) {
                        subtaskRateLimiter.acquire();
                        long timestamp = partitionState.extractTimestamp(record, kafkaEventTimestamp);
                        sourceContext.collectWithTimestamp(record, timestamp);

                        // this might emit a watermark, so do it after emitting the record
                        partitionState.onEvent(record, timestamp);
                    }
                    partitionState.setOffset(offset);
                }
            }

        };



    }


}
