package drools;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import javax.xml.bind.DatatypeConverter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.junit.Before;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.io.Resource;

import com.neustar.io.net.forward.ForwarderIfc;
import com.neustar.io.net.forward.rest.RestfulGetForwarder;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;
import io.rules.drools.StatelessRuleRunner;

public class TestDroolsAnsForwarders {
	
	
	
	//private String phoenix_zk_JDBC= "jdbc:phoenix:ec2-52-25-103-3.us-west-2.compute.amazonaws.com,ec2-52-36-108-107.us-west-2.compute.amazonaws.com:2181:/hbase-unsecure:hbase";
	Schema schema = null;
	@Before
	public void init() throws IOException{

		try {
			schema = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/versions/current/NeustarMessage.avsc").openStream());
			
			//schema = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/NeustarMessage.avsc"));
			
			/*
			ObjectMapper mapper = new ObjectMapper();
			Map<String,?> neuNode = mapper.readValue(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/NeustarMessage.avsc"), Map.class);
			
			neuNode.get("")
			JsonNode remoteNode = mapper.readTree(	new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/RemoteRequest.avsc"));

			schema = new Schema.Parser().parse(writer.toString());
			*/
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
 }
	 public void init1() throws IOException{

			try {
				//schema = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/versions/current/NeustarMessage.avsc").openStream());
				File [] files = new File[] {
						new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/NeustarMessage.avsc"),
						new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/RemoteRequest.avsc")
				};
				
				StringWriter writer = new StringWriter();
				
				
				for(File file : files){
					
					BufferedReader reader = new BufferedReader(new FileReader(file));
					String line = null;
					while(( line = reader.readLine()) !=null){
						writer.write(line);
					}
					
					reader.close();
					
				}
				
				
				schema = new Schema.Parser().parse(writer.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	 }

	 
	 @Test public void testRestGetCall() throws Throwable
	 {
		 	String restUri = "http://ec2-52-41-124-186.us-west-2.compute.amazonaws.com:8080";
			GenericRecord mesg = new GenericData.Record(schema);		
			mesg.put("id", "device1");
			mesg.put("payload", "owners/143");
			mesg.put("messagetype", "REGISTRY_GET");
			
			//create avro
			byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			Map<String,?> map =  parser.parse(avrodata, schema);
			
			ForwarderIfc restForward = RestfulGetForwarder.singleton(restUri);	
			String response = restForward.forward(map,schema);
			
			System.out.println("Sent message "+response);
			
	 }
	 
	 @Test public void testRestPostCall() throws Throwable
	 {

		 	Schema schema = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/NeustarMessage.avsc"));
			GenericRecord mesg = new GenericData.Record(schema);	
			/*
			mesg.put("sourceid", "device1");
			mesg.put("payload", "{\"id\":\"000000a9-2c7a-4654-8f34-f6e1d1ad8ad7/YS9saWdodA==\",\"data\":{\"value\":false}}");
			mesg.put("messagetype", "REGISTRY_PUT");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");
			*/
			mesg.put("sourceid", "device1");
			
			/*{"path":"a/light","verb":"POST","deviceId":"54919CA5-4101-4AE4-595B-353C51AA983C","statusCode":200,"txId":"98"}*/
			
			Schema schema_remoteReq = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/RemoteRequest.avsc"));
			
			GenericRecord remotemesg = new GenericData.Record(schema_remoteReq);	
			remotemesg.put("path", "a/light");
			remotemesg.put("payload","{\"value\":true,\"brightness\":30}");
			remotemesg.put("deviceId","54919CA5-4101-4AE4-595B-353C51AA983C");
			remotemesg.put("header","");
			remotemesg.put("txId","98");
			remotemesg.put("verb","POST");
			remotemesg.put("statusCode",200);
			
			byte[] payloadavro = AvroUtils.serializeJava(remotemesg, schema_remoteReq);
			GenericRecord genericPayload = AvroUtils.avroToJava(payloadavro, schema_remoteReq);
			mesg.put("registrypayload", genericPayload);
			mesg.put("payload", null);
			mesg.put("messagetype", "REGISTRY_POST");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");

			System.out.println(mesg.toString());
			//create avro
			//byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			byte[] avrodata = AvroUtils.serializeJava(mesg, schema);
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			
			Map<String,?> avromap =  parser.parse(avrodata, schema);

			runRules(avromap);
	
	 }
	 
	 @Test public void testPhoenixCallCall() throws Throwable
	 {

			GenericRecord mesg = new GenericData.Record(schema);	

			
			mesg.put("sourceid", "oneid");
			mesg.put("registrypayload", null);
			mesg.put("payload", "someting from oneid");
			mesg.put("messagetype", "TELEMETRY");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");			
			//create avro
			byte[] avrodata = AvroUtils.serializeJava(mesg, schema);
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			
			Map<String,?> avromap =  parser.parse(avrodata, schema);

			runRules4oneid(avromap);
	
	 }
	 
	 
	 
	 @Test public void testProcessRegistryResultCall() throws Throwable
	 {
		 	
		 	Schema schema = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/NeustarMessage_testing.avsc"));
			GenericRecord mesg = new GenericData.Record(schema);	

			mesg.put("sourceid", "device1");
			

			/*
			 * {"path":"/a/light","verb":"POST","payload":"{\"value\":true}","header":"hub-request","txId":"a37183ac-ba57-4213-a7f3-1c1608ded09e","deviceId":"RaspiLightUUID-Demo"}
			 * */
	
			Schema schema_remoteReq = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/RemoteRequest.avsc"));
			
			GenericRecord remotemesg = new GenericData.Record(schema_remoteReq);	
			/*remotemesg.put("path", "/api/v1/devices");
			remotemesg.put("payload","{\"value\":\"false\"}");
			remotemesg.put("deviceId","RaspiLightUUID-Demo");
			remotemesg.put("header","hub-request");
			remotemesg.put("txId","a37183ac-ba57-4213-a7f3-1c1608ded09e");
			remotemesg.put("verb","POST");*/
			
		
			remotemesg.put("path", "/a/light");
			remotemesg.put("payload","{\"value\":\"true\"}");
			remotemesg.put("deviceId","RaspiLightUUID-Demo");
			remotemesg.put("header","hub-request");
			remotemesg.put("txId","a37183ac-ba57-4213-a7f3-1c1608ded09e");
			remotemesg.put("verb","POST");
			
			byte[] payloadavro = AvroUtils.serializeJava(remotemesg, schema_remoteReq);
			GenericRecord genericPayload = AvroUtils.avroToJava(payloadavro, schema_remoteReq);
			mesg.put("registrypayload", genericPayload);
			mesg.put("payload", "{\"owner\"=\"kaniu\", \"test\"=\"Testing the format of this internal json\"}");
			mesg.put("messagetype", "REGISTRY_RESPONSE");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");

			System.out.println(mesg.toString());
			//create avro
			//byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			byte[] avrodata = AvroUtils.serializeJava(mesg, schema);
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			
			Map<String,?> avromap =  parser.parse(avrodata, schema);

			runRules(avromap);
			
	 }	 
	 
	 
	 @Test public void testRegistryPOSTCall() throws Throwable
	 {
		 	
		 	Schema schema = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/NeustarMessage.avsc"));
			GenericRecord mesg = new GenericData.Record(schema);	
			
			mesg.put("sourceid", "device1");
			mesg.put("payload", "{\"id\":\"000000a9-2c7a-4654-8f34-f6e1d1ad8ad7/YS9saWdodA==\",\"data\":{\"value\":false}}");
			mesg.put("messagetype", "REGISTRY_POST");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");
		
			mesg.put("sourceid", "device1");
			

			/*
			 * {"path":"/a/light","verb":"POST","payload":"{\"value\":true}","header":"hub-request","txId":"a37183ac-ba57-4213-a7f3-1c1608ded09e","deviceId":"RaspiLightUUID-Demo"}
			 * 
			 * */

			
		
			
			Schema schema_remoteReq = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/RemoteRequest.avsc"));
			
			GenericRecord remotemesg = new GenericData.Record(schema_remoteReq);	
			/*remotemesg.put("path", "/api/v1/devices");
			remotemesg.put("payload","{\"value\":\"false\"}");
			remotemesg.put("deviceId","RaspiLightUUID-Demo");
			remotemesg.put("header","hub-request");
			remotemesg.put("txId","a37183ac-ba57-4213-a7f3-1c1608ded09e");
			remotemesg.put("verb","POST");*/
			
		
			remotemesg.put("statusCode", 0);
			remotemesg.put("path", "/a/light");
			remotemesg.put("payload","{\"value\":\"true\"}");
			remotemesg.put("deviceId","RaspiLightUUID-Demo");
			remotemesg.put("header","hub-request");
			remotemesg.put("txId","a37183ac-ba57-4213-a7f3-1c1608ded09e");
			remotemesg.put("verb","POST");
			remotemesg.put("statusCode",0);
			
			byte[] payloadavro = AvroUtils.serializeJava(remotemesg, schema_remoteReq);
			GenericRecord genericPayload = AvroUtils.avroToJava(payloadavro, schema_remoteReq);
			mesg.put("registrypayload", genericPayload);
			
			System.out.println(mesg.toString());
			//create avro
			//byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			byte[] avrodata = AvroUtils.serializeJava(mesg, schema);
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			
			Map<String,?> avromap =  parser.parse(avrodata, schema);

			runRules(avromap);
			
	 }	 
	 @Test public void testRestPutCall() throws Throwable
	 {
		 	
		 	Schema schema = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/NeustarMessage.avsc"));
			GenericRecord mesg = new GenericData.Record(schema);	
			/*
			mesg.put("sourceid", "device1");
			mesg.put("payload", "{\"id\":\"000000a9-2c7a-4654-8f34-f6e1d1ad8ad7/YS9saWdodA==\",\"data\":{\"value\":false}}");
			mesg.put("messagetype", "REGISTRY_PUT");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");
			*/
			mesg.put("sourceid", "device1");
			
			/*{"deviceId":"54919CA5-4101-4AE4-595B-353C51AA983C","path":"/a/light","payload":{"value":true,"brightness":30}}*/
			
			
			Schema schema_remoteReq = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/RemoteRequest.avsc"));
			
			GenericRecord remotemesg = new GenericData.Record(schema_remoteReq);	
			remotemesg.put("path", "a/light");
			remotemesg.put("payload","{\"value\":true,\"brightness\":30}");
			remotemesg.put("deviceId","54919CA5-4101-4AE4-595B-353C51AA983C");
			remotemesg.put("header","");
			remotemesg.put("txId","");
			remotemesg.put("verb","");
			remotemesg.put("statusCode",0);
			
			byte[] payloadavro = AvroUtils.serializeJava(remotemesg, schema_remoteReq);
			GenericRecord genericPayload = AvroUtils.avroToJava(payloadavro, schema_remoteReq);
			mesg.put("registrypayload", genericPayload);
			mesg.put("payload", null);
			mesg.put("messagetype", "REGISTRY_PUT");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");

			System.out.println(mesg.toString());
			//create avro
			//byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			byte[] avrodata = AvroUtils.serializeJava(mesg, schema);
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			
			Map<String,?> avromap =  parser.parse(avrodata, schema);

			runRules(avromap);
			
	 }


	 private void runRules4oneid(Map<String,?>map){
			StatelessRuleRunner runner = new StatelessRuleRunner();
			String [] rules =  {"drools/RouteGenericMapDataRules_oneid.drl"};
			Resource resources [] = new Resource[rules.length];
			
			for(int i = 0 ; i < rules.length; i++){
				Resource resource = KieServices.Factory.get().getResources().newClassPathResource(rules[i]);
				resources[i]=resource;
			}

			Object [] facts = {map};
			Object[] ret = runner.runRules(resources, facts);
			
			
			String s = Arrays.toString(ret);
			System.out.println(s);
	 }
	 
	 private void runRules(Map<String,?>map){
			StatelessRuleRunner runner = new StatelessRuleRunner();
			String [] rules =  {"drools/RouteGenericMapDataRules_default.drl"};
			Resource resources [] = new Resource[rules.length];
			
			for(int i = 0 ; i < rules.length; i++){
				Resource resource = KieServices.Factory.get().getResources().newClassPathResource(rules[i]);
				resources[i]=resource;
			}

			Object [] facts = {map};
			Object[] ret = runner.runRules(resources, facts);
			
			
			String s = Arrays.toString(ret);
			System.out.println(s);
	 }
	 
	 @Test
	 public void base64Encoding(){
		 String path = "a/light";
		 String encoded = DatatypeConverter.printBase64Binary(path.getBytes());
		 
		 System.out.println("encoded = "+encoded+"  for "+path);
	 }
}
