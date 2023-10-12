package com.hzx.tuple;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;

/**
 * @Author: shuijun
 * @Date 2021/5/11 上午10:49
 * @Description
 */

public class Event implements BaseEvent {

    public Event() {
    }

    private String timestamp;
    private Metadata metadata;

    private String message;

    private Fields fields;

    private Host host;

    private Log log;

    public Log getLog() {
        return log;
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public Fields getFields() {
        return fields;
    }

    public void setFields(Fields fields) {
        this.fields = fields;
    }

    public Host getHost() {
        return host;
    }

    public void setHost(Host host) {
        this.host = host;
    }

    @JsonGetter("@timestamp")
    public String getTimestamp() {
        return timestamp;
    }

    @JsonSetter("@timestamp")
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }


    @JsonGetter("@metadata")
    public Metadata getMetadata() {
        return metadata;
    }

    @JsonSetter("@metadata")
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }


    @Override
    public String toString() {
        return "Event{" +
                "@timestamp='" + timestamp + '\'' +
                ", @metadata=" + metadata +
                ", message='" + message + '\'' +
                ", fields=" + fields +
                ", host=" + host +
                ", log=" + log +
                '}';
    }

    public static class Host {


        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }


        @Override
        public String toString() {
            return "Host{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }


    public static class Fields {
        private String topic;

        private String ip;

        private String name;

        private String center;

        private String clusterId;

        private String hostname;

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public String getClusterId() {
            return clusterId;
        }

        public void setClusterId(String clusterId) {
            this.clusterId = clusterId;
        }

        public String getCenter() {
            return center;
        }

        public void setCenter(String center) {
            this.center = center;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        @Override
        public String toString() {
            return "Fields{" +
                    "topic='" + topic + '\'' +
                    ", ip='" + ip + '\'' +
                    ", name='" + name + '\'' +
                    ", center='" + center + '\'' +
                    '}';
        }
    }

    public static class Log {

        private long offset;

        private FileName fileName;

        @JsonIgnore
        private String flags;

        public String getFlags() {
            return flags;
        }

        public void setFlags(String flags) {
            this.flags = flags;
        }

        @JsonGetter("offset")
        public long getOffset() {
            return offset;
        }

        @JsonSetter("offset")
        public void setOffset(long offset) {
            this.offset = offset;
        }

        @JsonGetter("file")
        public FileName getFileName() {
            return fileName;
        }

        @JsonSetter("file")
        public void setFileName(FileName fileName) {
            this.fileName = fileName;
        }

        @Override
        public String toString() {
            return "Log{" +
                    "offset=" + offset +
                    ", file=" + fileName +
                    '}';
        }
    }

    public static class FileName {
        private String path;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        @Override
        public String toString() {
            return "FileName{" +
                    "path='" + path + '\'' +
                    '}';
        }
    }

    public static class Metadata {
        private String beat;
        private String type;
        private String version;

        private String _id;

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        @Override
        public String toString() {
            return "Metadata{" +
                    "beat='" + beat + '\'' +
                    ", type='" + type + '\'' +
                    ", version='" + version + '\'' +
                    ", _id='" + _id + '\'' +
                    '}';
        }

        public String getBeat() {
            return beat;
        }

        public void setBeat(String beat) {
            this.beat = beat;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }
}
