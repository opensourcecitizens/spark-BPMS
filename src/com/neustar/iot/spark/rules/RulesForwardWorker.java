package com.neustar.iot.spark.rules;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.neustar.iot.spark.forward.ForwarderIfc;
import com.neustar.iot.spark.forward.phoenix.PhoenixForwarder;
import com.neustar.iot.spark.forward.rest.RestfulGetForwarder;
import com.neustar.iot.spark.forward.rest.RestfulPutForwarder;

/**This class abstracts forwarders from rules engine. 
 * This class will be instantiated in drools and return string values for reporting.
 * */
public class RulesForwardWorker implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String avro_schema_hdfs_location = null;
	private Properties properties = null;

	public RulesForwardWorker() {

	}

	protected void loadPropertiesFromDB() throws IOException {
		InputStream props = RulesForwardWorker.class.getClassLoader().getResourceAsStream("consumer.props");
		properties = new Properties();
		properties.load(props);

		if (properties.getProperty("group.id") == null) {
			properties.setProperty("group.id", "group-localtest");
		}

		avro_schema_hdfs_location = properties.getProperty("avro.schema.hdfs.location");
	}

	protected Schema readSchemaFromHDFS(Schema.Parser parser, String uri) throws IOException {

		loadPropertiesFromDB();

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;

		Schema ret = null;
		try {
			in = fs.open(new Path(uri));
			ret = parser.parse(in);
		} finally {
			IOUtils.closeStream(in);
		}
		return ret;
	}

	protected Schema retrieveLatestAvroSchema() throws IOException {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = readSchemaFromHDFS(parser, avro_schema_hdfs_location);// parser.parse(SimpleAvroProducer.USER_SCHEMA);
		return schema;
	}

	public String writeToDB(Map<String, ?> map, String phoenix_zk_JDBC, Map<String, ?> attr) {

		try {
			ForwarderIfc phoenixConn = PhoenixForwarder.singleton(phoenix_zk_JDBC);
			Schema schema = retrieveLatestAvroSchema();
			return phoenixConn.forward(map, schema);
		} catch (Throwable e) {
			return e.getMessage();
		}

	}

	public String remoteRestPut(Map<String, ?> map, String rest_Uri, Map<String, ?> attr) {
		try {

			ForwarderIfc forwarder = RestfulPutForwarder.singleton(rest_Uri);
			Schema schema = retrieveLatestAvroSchema();
			return forwarder.forward(map, schema);
		} catch (Throwable e) {
			return e.getMessage();
		}
	}

	public String remoteRestGet(Map<String, ?> map, String rest_Uri, Map<String, ?> attr) {

		try {

			ForwarderIfc forwarder = RestfulGetForwarder.singleton(rest_Uri);
			Schema schema = retrieveLatestAvroSchema();
			return forwarder.forward(map, schema);
		} catch (Throwable e) {
			return e.getMessage();
		}
	}

	static Map<String, String> rulesTempDB = new HashMap<String, String>() {
		{
			put("default", "drools/RouteGenericMapDataRules_kaniu.drl");
			put("kaniu", "drools/RouteGenericMapDataRules_kaniu.drl");
			put("customer1", "drools/RouteGenericMapDataRules_customer1.drl");
			put("customer2", "drools/RouteGenericMapDataRules-customer2.drl");
		}
	};

	/**
	 * Database query for the drools rules by userid
	 **/
	public static InputStream retrieveRules(String uniqueId) {

		uniqueId = rulesTempDB.containsKey(uniqueId) ? uniqueId : "default";

		InputStream rulesStream = RulesForwardWorker.class.getClassLoader()
				.getResourceAsStream(rulesTempDB.get(uniqueId));

		return rulesStream;
	}

}
