package com.hzx.tuple;

import com.hzx.avro.AvroBean;
import com.hzx.avro.AvroRecordHandler;
import com.hzx.avro.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
/*
 * Author: 47035
 * Date: 2023/10/11 10:51
 * FileName: AvroDeserializationSchema
 * Description:
 */


public class AvroDeserializationSchema implements DeserializationSchema<List<AvroBean>> {
	
	private final static String avroName= "avro.avsc";
	
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
	
	private static Schema getSchema() {
		Schema schema = loadAvroSchemaFromResource();
		return  schema;
	}
	
	@Override
	public List<AvroBean> deserialize(byte[] bytes) throws IOException {
		List<AvroBean> receiveData = Lists.newArrayList();
		ByteArrayInputStream bis = new ByteArrayInputStream((bytes));
		BinaryDecoder bd = DecoderFactory.get().binaryDecoder(bis, null);
		DatumReader<GenericRecord> dataReader =
				new GenericDatumReader(getSchema());
		while (!bd.isEnd()) {
			GenericRecord dxxRecord = dataReader.read(null, bd);
			AvroBean partialCdr = AvroRecordHandler.deSerializeRecord(dxxRecord);
			receiveData.add(partialCdr);
		}
		
		
		return receiveData;
	}
	
	@Override
	public boolean isEndOfStream(List<AvroBean> avroBeans) {
		return false;
	}
	
	@Override
	public TypeInformation<List<AvroBean>> getProducedType() {
		return null;
	}
}