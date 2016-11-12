package avro;

import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import com.neustar.iot.spark.kafka.SecurityAndAvroStandardizationStreamProcess;
import com.neustar.iot.spark.rules.RulesForwardWorker;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;

public class TestEnd2EndAvro {
	Schema schema = null;
	Schema schema_remoteReq = null;
	//String schemaUrl = "https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/versions/0.0.1_20160928/NeustarMessage.avsc";
	@Before
	public void init() throws IOException{
	 //schema = new Schema.Parser().parse(new URL(schemaUrl).openStream());
	 
	 schema_remoteReq = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/registry-to-spark/versions/current/remoterequest.avsc").openStream());
		
	 schema = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/versions/current/NeustarMessage.avsc").openStream());

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
		Map<String,?> jsonMap = new SecurityAndAvroStandardizationStreamProcess("device.out",1,"").parseJsonData(jsondata.getBytes());

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
	
	@Test
	public void testCreateAndSearchMap() throws Exception{
		GenericRecord remotemesg = new GenericData.Record(schema_remoteReq);	
		remotemesg.put("path", "/api/v1/devices");
		remotemesg.put("payload","{\"value\":\"false\"}");
		remotemesg.put("deviceId","000000a9-2c7a-4654-8f34-f6e1d1ad8ad7/YS9saWdodA==");
		remotemesg.put("header","{\"API-KEY\": \"0\",\"Content-Type\": \"application/json\"}");
		remotemesg.put("txId","someTextid");
		remotemesg.put("verb","update");
		
		byte[] payloadavro = AvroUtils.serializeJava(remotemesg, schema_remoteReq);
		GenericRecord genericPayload = AvroUtils.avroToJava(payloadavro, schema_remoteReq);
		
			
	 	GenericRecord mesg = new GenericData.Record(schema);	
		mesg.put("sourceid", "device1");
		mesg.put("registrypayload", genericPayload);
		mesg.put("payload", "");
		mesg.put("messagetype", "TELEMETRY");
		mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
		mesg.put("messageid", UUID.randomUUID()+"");
		
		byte[] parentpayload = AvroUtils.serializeJava(mesg, schema);
		
		AvroParser<Map<String,?>> avroParser = new AvroParser<Map<String,?>>(schema);
		Map<String,?> map = avroParser.parse(parentpayload, new HashMap<String,Object>());	
		System.out.println(new RulesForwardWorker().searchMapFirstSubKey("RemoteRequest", map));
	}
	
	@Test public void testProcess() throws JsonGenerationException, JsonMappingException, IOException, ExecutionException{
		/*{"path":"/a/light","verb":"POST","payload":{ "value": true },"statusCode":2,"txId":"396790e1-09c8-409a-9274-37e578dc5d4e"}*/
		
		Map<String, Object> data = new HashMap<String,Object>();
		data.put("path", "/a/light");
		data.put("verb", "POST");
		data.put("payload", "{ \"value\": true }");
		data.put("statusCode", 2);
		data.put("txId", "396790");
		ObjectMapper mapper = new ObjectMapper();
		String jsondata = mapper.writeValueAsString(data);
		
		//message sent over to kafka
		
		//security spark process : message recieved from  kafka in 
		//Map<String,?> jsonMap = new SecurityAndAvroStandardizationStreamProcess("device.out",1,"").parseJsonData(jsondata.getBytes());
		SecurityAndAvroStandardizationStreamProcess proc = new SecurityAndAvroStandardizationStreamProcess("device.out",1,"");
		proc.createAndSendAvroToQueue(data,proc.getProps());
		
	}
	
	@Test public void testProcess2() throws JsonGenerationException, JsonMappingException, IOException, ExecutionException{
		String jsondata = "{\"deviceId\" : \"54919CA5-4101-4AE4-595B-353C51AA983C\",\"path\" : \"/a/light\","
				+ " \"payload\" : {\"value\" : true,\"brightness\" : 30 }}";
		
		
		Map<String, Object> data = new HashMap<String,Object>();
		data.put("deviceId", "54919CA5-4101-4AE4-595B-353C51AA983C");
		data.put("path", "/a/light");
		data.put("payload", "{\"value\" : true,\"brightness\" : 30}");

		//ObjectMapper mapper = new ObjectMapper();
		//String jsondata = mapper.writeValueAsString(data);
		ObjectMapper mapper = new ObjectMapper();
		data = mapper.readValue(jsondata, HashMap.class);
		//message sent over to kafka
		
		//security spark process : message recieved from  kafka in 
		//Map<String,?> jsonMap = new SecurityAndAvroStandardizationStreamProcess("device.out",1,"").parseJsonData(jsondata.getBytes());
		SecurityAndAvroStandardizationStreamProcess proc = new SecurityAndAvroStandardizationStreamProcess("device.event",1,"");
		proc.createAndSendAvroToQueue(data,proc.getProps());
		
	}
}
