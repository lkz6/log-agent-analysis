package com.hzx.avro;

import org.apache.avro.generic.GenericRecord;

public class AvroRecordHandler {

    public static AvroBean deSerializeRecord(GenericRecord cdrRecord) {
        AvroBean avroBean = new AvroBean();
        avroBean.setOffset(asLong(cdrRecord.get("offset")));
        avroBean.setTimestamp(asLong(cdrRecord.get("timestamp")));
        avroBean.setTopic(asString(cdrRecord.get("topic")));
        avroBean.setMessage(asString(cdrRecord.get("message")));
        avroBean.setIp(asString(cdrRecord.get("ip")));
        avroBean.setCenter(asString(cdrRecord.get("center")));
        avroBean.setName(asString(cdrRecord.get("name")));
        return avroBean;
    }

    private static  Integer asInteger(Object o) {
        if(o!=null){
            if (o instanceof Long)
            {
                return ((Long) o).intValue();
            }
            return (Integer) o;
        }
        return null;
    }

    /**
     * Object 转为 Long
     * @param o
     * @return
     */
    private static  Long asLong(Object o) {
        if(o!=null){
            if (o instanceof Integer)
            {
                return ((Integer) o).longValue();
            }
            return (Long) o;
        }
        return null;
    }

    public static String asString(Object o){
        if(o == null){
            return null;
        }
        return String.valueOf(o);
    }
}
