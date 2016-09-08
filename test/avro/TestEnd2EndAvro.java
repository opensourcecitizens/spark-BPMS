package avro;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import com.neustar.iot.spark.kafka.SecurityAndAvroStandardizationStreamProcess;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;

public class TestEnd2EndAvro {
	Schema schema = null;
	@Before
	public void init() throws IOException{
	 schema = new Schema.Parser().parse(Class.class.getResourceAsStream("/CustomMessage.avsc"));
	}
	
	String message = "{\"deviceName\":\"Testing a very large avro message by adding this sentence: Maya's Monster Inc. Lamp Annd a very long sentence that makes this message even bigger for testing payload capacity\"}";
	@Test public void endtoendTest() throws Exception{
		//gateway message construction
		Map<String, Object> data = new HashMap<String,Object>();
		data.put("payload", message);
		data.put("sourceid", "yaima");
		data.put("authentication", "someid");
		data.put("messagetype", "TELEMETRY");
		ObjectMapper mapper = new ObjectMapper();
		String jsondata = mapper.writeValueAsString(data);
		
		//message sent over to kafka
		
		//security spark process : message recieved from  kafka in 
		Map<String,?> jsonMap = new SecurityAndAvroStandardizationStreamProcess(null,1,null).parseJsonData(jsondata.getBytes());
			//extract payload etc and create avro message
		System.out.println(jsonMap);;
		//create generic avro record 
		

		GenericRecordBuilder outMap =  new GenericRecordBuilder(schema);
		String sourceid = (String) jsonMap.get("sourceid");
		String time = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date());
		String msgid = UUID.randomUUID()+sourceid;//fromString( sourceid+time ).toString();
		
		outMap.set("messageid", msgid);
		outMap.set("sourceid", sourceid);
		outMap.set("payload", jsonMap.get("payload"));
		outMap.set("createdate", time);
		outMap.set("messagetype", jsonMap.get("messagetype"));
		
		//create avro
		byte[] avro = AvroUtils.serializeJava(outMap.build(), schema);

		int size = 0;
		for(int i = 0 ; i < avro.length; i++){
			size += avro[i];	
		}
		
		System.out.println();
		//send to kafka
		System.out.println("Avro size = "+size);
		String i = "12345MS";
		System.out.println("int size = "+Bytes.toBytes(i).length);
		//business process: receive from kafka
			//parse avro data to map for analysis
		AvroParser<Map<String,?>> avroParser = new AvroParser<Map<String,?>>(schema);
		Map<String,?> mapdata =  avroParser.parse(avro, new HashMap<String,Object>());	
			//send map to elasticsearch
		
		System.out.println(mapdata);;
		
		
	}

	@Test public void byteArrayConversion() throws IOException{
		
		String byteStr= "72,52,57,53,49,57,101,98,50,45,56,101,52,48,45,52,100,53,57,45,97,101,53,49,45,54,51,48,50,50,99,49,99,50,54,57,48,10,121,97,105,109,97";//,-96,        2,106,117,115,116,32,116,101,115,116,105,110,103,32,97,32,115,101,110,116,101,110,99,101,32,119,105,116,104,32,77,97,121,97,39,115,32,77,111,110,115,116,101,114,32,73,110,99,46,32,76,97,109,112,32,65,110,110,100,32,97,32,118,101,114,121,32,108,111,110,103,32,115,101,110,116,101,110,99,101,32,116,104,97,116,32,109,97,107,101,115,32,116,104,105,115,32,109,101,115,115,97,103,101,32,101,118,101,110,32,98,105,103,103,101,114,32,102,111,114,32,116,101,115,116,105,110,103,32,112,97,121,108,111,97,100,32,99,97,112,97,99,105,116,121,4,22,83,101,112,32,54,44,32,50,48,49,54";
		String byteStrM="72,52,57,53,49,57,101,98,50,45,56,101,52,48,45,52,100,53,57,45,97,101,53,49,45,54,51,48,50,50,99,49,99,50,54,57,48,10,121,97,105,109,97,-17,-65,-67,2,106,117,115,116,32,116,101,115,116,105,110,103,32,97,32,115,101,110,116,101,110,99,101,32,119,105,116,104,32,77,97,121,97,39,115,32,77,111,110,115,116,101,114,32,73,110,99,46,32,76,97,109,112,32,65,110,110,100,32,97,32,118,101,114,121,32,108,111,110,103,32,115,101,110,116,101,110,99,101,32,116,104,97,116,32,109,97,107,101,115,32,116,104,105,115,32,109,101,115,115,97,103,101,32,101,118,101,110,32,98,105,103,103,101,114,32,102,111,114,32,116,101,115,116,105,110,103,32,112,97,121,108,111,97,100,32,99,97,112,97,99,105,116,121,4,22,83,101,112,32,54,44,32,50,48,49,54";
	
		String [] byteStrArr = byteStr.split(",");
		byte[] bytes = new byte[byteStrArr.length];
		
		for(int i = 0 ; i < byteStrArr.length; i++){
			bytes[i] = Byte.parseByte(byteStrArr[i].trim());
		}
		
		String json = AvroUtils.avroToJson(bytes,  schema);
		
		System.out.println(json);
	}
}
