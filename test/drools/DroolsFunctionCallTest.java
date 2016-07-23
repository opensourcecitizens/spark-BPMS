package drools;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.io.Resource;
import org.kie.api.io.ResourceType;

import com.neustar.iot.spark.rules.RulesForwardWorker;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;
import io.rules.drools.StatelessRuleRunner;

public class DroolsFunctionCallTest {
	Map<String,?> map = null;
	@Before public void createMap() throws Exception{
		Schema schema = new Schema.Parser().parse(DroolsFunctionCallTest.class.getResourceAsStream("/drools/CustomMessage.avsc"));

		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "customer1");
		mesg.put("payload", "owners/143");//registry get
		mesg.put("messagetype","NOTIFICATION");
		//create avro
		byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
		
		//avro to map
		AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
		map =  parser.parse(avrodata, schema);
	}
	@Test public void testRegistryGet(){
		
		
		//InputStream rulesStream = DroolsFunctionCallTest.class.getResourceAsStream("drools/RouteGenericMapDataRules.drl");
		StatelessRuleRunner runner = new StatelessRuleRunner();
		//String [] rules =  {"drools/RouteGenericMapDataRules.drl"};
		
		Resource resource = KieServices.Factory.get().getResources().newClassPathResource("drools/RouteGenericMapDataRules.drl");//ByteArrayResource(rulesStream,"UTF-8");
		Resource [] rules = {resource};
		Object [] facts = {map};
		Object[] ret = runner.runRules(rules, facts);
		
		
		String s = Arrays.toString(ret);
		System.out.println(s);
		//assertEquals(p ,false);
	}
	
	@Test public void testRulesByUniqueId() throws IOException{
		
		String customerId = (String) map.get("id");
		System.out.println("uniqueid = "+customerId);;
		InputStream rulesStream = RulesForwardWorker.retrieveRules(customerId);
		StringWriter writer = new StringWriter();
		IOUtils.copy(rulesStream, writer);
		
		StatelessRuleRunner runner = new StatelessRuleRunner();
		
		Resource resource = KieServices.Factory.get().getResources().newByteArrayResource(writer.toString().getBytes(),"UTF-8");
		resource.setTargetPath("src/main/resources/"+customerId+"customer_drl");
		resource.setResourceType(ResourceType.DRL );
		
		Resource [] rules = {resource};
		Object [] facts = {map};
		Object[] ret = runner.runRules(rules, facts);	
		
		String s = Arrays.toString(ret);
		System.out.println(s);
		
		
	}
}
