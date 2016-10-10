package drools;
import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.io.Resource;

import com.neustar.iot.spark.forward.ForwarderIfc;
import com.neustar.iot.spark.forward.phoenix.PhoenixForwarder;
import com.neustar.iot.spark.forward.rest.ElasticSearchPostForwarder;
import com.neustar.iot.spark.forward.rest.RestfulGetForwarder;
import com.neustar.iot.spark.forward.rest.RestfulPostForwarder;
import com.neustar.iot.spark.forward.rest.RestfulPutForwarder;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;
import io.rules.drools.StatelessRuleRunner;

public class TestDroolsAnsForwarders {
	
	
	
	private String phoenix_zk_JDBC= "jdbc:phoenix:ec2-52-25-103-3.us-west-2.compute.amazonaws.com,ec2-52-36-108-107.us-west-2.compute.amazonaws.com:2181:/hbase-unsecure:hbase";
	Schema schema = null;
	@Before
	 public void init() throws IOException{

			try {
				schema = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/versions/current/NeustarMessage.avsc").openStream());
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

			GenericRecord mesg = new GenericData.Record(schema);	

			mesg.put("sourceid", "device1");
			mesg.put("payload", "{\"relativeHref\":\"/a/light\", \"desiredState\":{\"value\":false}}");
			mesg.put("messagetype", "REGISTRY_POST");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");
			

			//create avro
			byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			
			Map<String,?> map =  parser.parse(avrodata, schema);
			
			runRules(map);
	
	 }
	 
	 @Test public void testRestPutCall() throws Throwable
	 {
		 	
			GenericRecord mesg = new GenericData.Record(schema);	

			mesg.put("sourceid", "device1");
			mesg.put("payload", "{\"id\":\"000000a9-2c7a-4654-8f34-f6e1d1ad8ad7/YS9saWdodA==\",\"data\":{\"value\":false}}");
			mesg.put("messagetype", "REGISTRY_PUT");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");
			

			//create avro
			byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			
			Map<String,?> map =  parser.parse(avrodata, schema);

			runRules(map);
			
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
}
