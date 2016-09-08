import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

import com.neustar.iot.spark.forward.ForwarderIfc;
import com.neustar.iot.spark.forward.phoenix.PhoenixForwarder;
import com.neustar.iot.spark.forward.rest.ElasticSearchPostForwarder;
import com.neustar.iot.spark.forward.rest.RestfulGetForwarder;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;

public class TestForwarder {
	
	
	
	private String phoenix_zk_JDBC= "jdbc:phoenix:ec2-52-25-103-3.us-west-2.compute.amazonaws.com,ec2-52-36-108-107.us-west-2.compute.amazonaws.com:2181:/hbase-unsecure:hbase";
	Schema schema = null;
	@Before
	 public void init() throws IOException{
			schema = new Schema.Parser().parse(Class.class.getResourceAsStream("/CustomMessage.avsc"));
	 }

	 @Test public void testPhoenixWrite() throws Throwable
	 {
		 	
			GenericRecord mesg = new GenericData.Record(schema);		

			mesg.put("sourceid", "device1");
			mesg.put("payload", "{'type':'internal json'}");
			mesg.put("messagetype", "EXCEPTION");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");
			
			//create avro
			byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			Map<String,?> map =  parser.parse(avrodata, schema);
			
			PhoenixForwarder phoenixConn = PhoenixForwarder.singleton(phoenix_zk_JDBC);	
			phoenixConn.forward(map,schema);

			System.out.println("Sent message");
			
	 }
	 
	 @Test public void testRestCall() throws Throwable
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

	 @Test public void testRestElasticSearchCall() throws Throwable
	 {
		 	String restUri = "https://search-iot-logs-v1-piyxzjyhtd3abhkakgrgqjerh4.us-west-2.es.amazonaws.com/firebaseioindex/events";
			GenericRecord mesg = new GenericData.Record(schema);		
			mesg.put("sourceid", "device1");
			mesg.put("payload", "{'type':'internal json'}");
			mesg.put("messagetype", "EXCEPTION");
			mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
			mesg.put("messageid", UUID.randomUUID()+"");
			
			//create avro
			byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			Map<String,?> map =  parser.parse(avrodata, schema);
			
			ForwarderIfc restForward = ElasticSearchPostForwarder.singleton(restUri);	
			String response = restForward.forward(map,schema);
			
			System.out.println("Sent message "+response);
			
	 }
	 
	 
	 @Test public void postToReg(){
		 
	 }
}
