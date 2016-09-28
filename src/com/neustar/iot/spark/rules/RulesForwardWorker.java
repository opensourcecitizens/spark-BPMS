package com.neustar.iot.spark.rules;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.neustar.iot.spark.AbstractStreamProcess;
import com.neustar.iot.spark.forward.ForwarderIfc;
import com.neustar.iot.spark.forward.phoenix.PhoenixForwarder;
import com.neustar.iot.spark.forward.rest.ElasticSearchPostForwarder;
import com.neustar.iot.spark.forward.rest.RestfulGetForwarder;
import com.neustar.iot.spark.forward.rest.RestfulPostForwarder;
import com.neustar.iot.spark.forward.rest.RestfulPutForwarder;

/**This class abstracts forwarders from rules engine. 
 * This class will be instantiated in drools and return string values for reporting.
 * */
public class RulesForwardWorker extends AbstractStreamProcess implements Serializable {
	private static final Logger log = Logger.getLogger(RulesForwardWorker.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String EXCEPTION = "EXCEPTION_";
	private URL avro_schema_web_url = null;
	private String user_rules_hdfs_location = null;
	private Properties properties = null;

	public RulesForwardWorker() {
		try {
			loadPropertiesFromDB();
		} catch (IOException e) {
			log.error(e);
		}
	}

	protected void loadPropertiesFromDB() throws IOException {
		InputStream props = RulesForwardWorker.class.getClassLoader().getResourceAsStream("consumer.props");
		properties = new Properties();
		properties.load(props);

		if (properties.getProperty("group.id") == null) {
			properties.setProperty("group.id", "group-localtest");
		}

		user_rules_hdfs_location = properties.getProperty("rules.hdfs.location");
		avro_schema_web_url = properties.getProperty("avro.schema.web.url")!=null?new URL(properties.getProperty("avro.schema.web.url")):null;
		
	}

	protected Schema readSchemaFromLocal(Schema.Parser parser) throws IOException{
		//loadPropertiesFromDB();
		
		InputStream in = RulesForwardWorker.class.getClassLoader().getResourceAsStream("CustomMessage.avsc");

		Schema ret = null;
		try {
			ret = parser.parse(in);
		} finally {
			IOUtils.closeStream(in);
		}
		return ret;
	}	

	protected Schema retrieveLatestAvroSchema() throws IOException, ExecutionException {
		
		Schema.Parser parser = new Schema.Parser();
		if(System.getenv("TEST")!=null && System.getenv("TEST").equalsIgnoreCase("TRUE"))return readSchemaFromLocal(parser);
		
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url);
		return schema;
	}

	public String writeToDB(String phoenix_zk_JDBC,Map<String, ?> map, Map<String, ?> attr ) {

		try {
			ForwarderIfc phoenixConn = PhoenixForwarder.instance(phoenix_zk_JDBC);
			Schema schema = retrieveLatestAvroSchema();
			return phoenixConn.forward(map, schema, attr);
		} catch (Throwable e) {
			log.error(e);
			return EXCEPTION+e.getMessage();
		}

	}

	public String remoteRestPut(String rest_Uri, Map<String, ?> map,  Map<String, ?> attr) {
		try {

			ForwarderIfc forwarder = RestfulPutForwarder.instance(rest_Uri);
			Schema schema = retrieveLatestAvroSchema();
			return forwarder.forward(map, schema, attr);
		} catch (Throwable e) {
			log.error(e);
			return EXCEPTION+e.getMessage();
		}
	}

	public String remoteRestGet(String rest_Uri, Map<String, ?> map,  Map<String, ?> attr) {

		try {

			ForwarderIfc forwarder = RestfulGetForwarder.instance(rest_Uri);
			Schema schema = retrieveLatestAvroSchema();
			return forwarder.forward(map, schema,attr);
		} catch (Throwable e) {
			log.error(e,e);
			return EXCEPTION+e.getMessage();
		}
	}
	
	public String remoteRestPost(String rest_Uri, Map<String, ?> map,  Map<String, ?> attr) {

		try {

			ForwarderIfc forwarder = RestfulPostForwarder.instance(rest_Uri);
			Schema schema = retrieveLatestAvroSchema();
			return forwarder.forward(map, schema, attr);
		} catch (Throwable e) {
			log.error(e,e);
			return EXCEPTION+e.getMessage();
		}
	}
	
	public String remoteElasticSearchPost (String rest_Uri, Map<String, ?> map,  Map<String, ?> attr) {

		try {

			ForwarderIfc forwarder = ElasticSearchPostForwarder.instance(rest_Uri);
			Schema schema = retrieveLatestAvroSchema();
			return forwarder.forward(map, schema, attr);
		} catch (Throwable e) {
			log.error(e,e);
			return EXCEPTION+e.getMessage();
		}
	}

	static Map<String, String> rulesTempDB = new HashMap<String, String>() {
		{
			put("default", "drools/RouteGenericMapDataRules_default.drl");
			put("kaniu", "drools/RouteGenericMapDataRules_kaniu.drl");
			put("yaima", "drools/RouteGenericMapDataRules_yaima.drl");
			put("customer1", "drools/RouteGenericMapDataRules_customer1.drl");
			put("customer2", "drools/RouteGenericMapDataRules-customer2.drl");
		}
	};

	/**
	 * Database query for the drools rules by userid
	 **/
	public InputStream retrieveRules(String uniqueId) {

		uniqueId = rulesTempDB.containsKey(uniqueId) ? uniqueId : "default";

		InputStream rulesStream = RulesForwardWorker.class.getClassLoader()
				.getResourceAsStream(rulesTempDB.get(uniqueId));

		return rulesStream;
	}
	
	public InputStream retrieveRulesFromHDFS(String uniqueId) throws IOException {
		
		uniqueId = rulesTempDB.containsKey(uniqueId) ? uniqueId : "default";
		
		String uri = user_rules_hdfs_location+"/"+rulesTempDB.get(uniqueId);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		return fs.open(new Path(uri));

	}

}
