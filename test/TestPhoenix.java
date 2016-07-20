import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

import com.neustar.iot.spark.phoenix.PhoenixForwarder;

import avro.TestAvro;
import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;

public class TestPhoenix {
	
	
	
	private String phoenix_zk_JDBC= "jdbc:phoenix:ec2-52-25-103-3.us-west-2.compute.amazonaws.com,ec2-52-36-108-107.us-west-2.compute.amazonaws.com:2181:/hbase-unsecure:hbase";
	Schema schema = null;
	@Before
	 public void init() throws IOException{
			schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
	 }

	 @Test public void testWrite() throws Exception
	 {
		 
			GenericRecord mesg = new GenericData.Record(schema);		
			mesg.put("id", "device1");
			mesg.put("payload", "{'type':'internal json'}");
			
			//create avro
			byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			Map<String,?> map =  parser.parse(avrodata, schema);
			
			
			PhoenixForwarder<String> phoenixConn = PhoenixForwarder.singleton(phoenix_zk_JDBC, new String());	
			//phoenixConn.saveToJDBC(map,schema);
			phoenixConn.saveToJDBC(map);
			System.out.println("Sent message");
	 }
}
