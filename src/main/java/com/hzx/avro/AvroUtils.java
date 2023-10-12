package com.hzx.avro;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.compress.utils.Lists;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class AvroUtils {

    private final static String avroName= "avro.avsc";

    private static Map<String,Schema> schemaMap = new ConcurrentHashMap<>();





    private static Schema getSchema(String topic) {
        Schema schema = schemaMap.get(topic);
        if(schema == null){
            schema =  loadAvroSchemaFromResource();
            schemaMap.put(topic,schema);
        }
        return  schema;
    }




    private static Schema loadAvroSchemaFromResource()  {
        ClassLoader classLoader = AvroUtils.class.getClassLoader();

        // 使用ClassLoader加载resources目录下的avsc文件
        try (InputStream inputStream = classLoader.getResourceAsStream(avroName)) {
            if (inputStream != null) {
                // 使用Avro的Schema.Parser解析avsc文件内容并生成Schema对象
                Schema.Parser parser = new Schema.Parser();
                return parser.parse(inputStream);
            } else {
                throw new IOException("Avsc file not found: " + avroName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static  List<AvroBean> doSerialize(byte[] bytes,String topic ) throws IOException{
        List<AvroBean> receiveData = Lists.newArrayList();
        ByteArrayInputStream bis = new ByteArrayInputStream((bytes));
        BinaryDecoder bd = DecoderFactory.get().binaryDecoder(bis, null);
        DatumReader<GenericRecord> dataReader =
                new GenericDatumReader(getSchema(topic));
        while (!bd.isEnd()) {
            GenericRecord dxxRecord = dataReader.read(null, bd);
            AvroBean partialCdr = AvroRecordHandler.deSerializeRecord(dxxRecord);
            receiveData.add(partialCdr);
        }
        return receiveData;
    }
}
