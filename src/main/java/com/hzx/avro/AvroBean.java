package com.hzx.avro;

import com.hzx.tuple.BaseEvent;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class AvroBean implements BaseEvent {
    private long timestamp;
    private long offset;
    private String topic;
    private String message;
    private String ip;
    private String center;
    private String name;
}
