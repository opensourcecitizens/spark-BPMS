package com.neustar.iot.spark.rules;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.neustar.iot.spark.AbstractStreamProcess;
import com.neustar.iot.spark.forward.ForwarderIfc;
import com.neustar.iot.spark.forward.mqtt.MQTTForwarder;
import com.neustar.iot.spark.forward.phoenix.PhoenixForwarder;
import com.neustar.iot.spark.forward.rest.ElasticSearchPostForwarder;
import com.neustar.iot.spark.forward.rest.RestfulGetForwarder;
import com.neustar.iot.spark.forward.rest.RestfulPostForwarder;
import com.neustar.iot.spark.forward.rest.RestfulPutForwarder;

/**
 * This class abstracts forwarders from rules engine. 
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
	private URL registry_avro_schema_web_url = null;
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
		registry_avro_schema_web_url = properties.getProperty("registry.avro.schema.web.url")!=null?new URL(properties.getProperty("registry.avro.schema.web.url")):null;
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
		
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url);
		return schema;
	}
	
	protected Schema retrieveLatestRegistryAvroSchema() throws IOException, ExecutionException {
		
		Schema schema = retrieveLatestAvroSchema(registry_avro_schema_web_url);
		return schema;
	}

	public String writeToDB(String phoenix_zk_JDBC,String tablename , Map<String, ?> map, Map<String, ?> attr ) {

		try {
			ForwarderIfc phoenixConn = PhoenixForwarder.instance(phoenix_zk_JDBC,tablename);
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
			log.error(e,e);
			e.printStackTrace();
			return EXCEPTION+ExceptionUtils.getStackTrace(e);
		}
	}
	
	
	public String remoteMQTTCall(String brokerUri,String clientId, Map<String, ?> map,  Map<String, ?> attr, Boolean ... registry) {
		try {
			
			Schema schema = null;
			if(registry==null || registry[0]==false)
				schema = retrieveLatestAvroSchema();
			else
				schema = retrieveLatestRegistryAvroSchema();
			
			ForwarderIfc forwarder = new MQTTForwarder(brokerUri,clientId);
			
			return forwarder.forward(map, schema, attr);
		} catch (Throwable e) {
			log.error(e);
			return EXCEPTION+e.getMessage();
		}
	}

	
	public String subscribeToMQTT(String brokerUri,String clientId,  Map<String, ?> attr, Integer ... timeout ) {
		try {

			int pollingtimeout = timeout!=null&&timeout[0]!=0?timeout[0]:6000;
			MQTTForwarder forwarder = new MQTTForwarder(brokerUri,clientId);
			forwarder.subscribe(attr);
			byte[] results = forwarder.pollMessage(pollingtimeout);//wait for 6 seconds
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(results, String.class);
			//return results;
		} catch (Throwable e) {
			log.error(e);
			//return EXCEPTION+e.getMessage();
		}
		return null;
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
	
	public String getSecurityToken(String userid){
		return "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6Ik1UVTRRVFZEUTBFMFJVSXhSRU5HTWpCR01FRTBPRGMwUTBRMU5VUkVOelV4T1VKRE1UQkZOQSJ9.eyJyb2xlIjoiSU5URVJOQUxfU0VSVklDRSIsInByaW1hcnlJZCI6NDkzLCJpc3MiOiJodHRwczovL2JhZ2RpLmF1dGgwLmNvbS8iLCJzdWIiOiJhdXRoMHw1ODEzYjMzY2YxNDEzYmVkMDk1MGU3ZjMiLCJhdWQiOiJHNGtlQU80bzBjbUV4ckw3YUtTSTk3RngzZXdMU3NTSyIsImV4cCI6MjQ3NzY4NjE1NSwiaWF0IjoxNDc3Njg2MTU1fQ.t0ogfhyqLeIzoilgiT2svQGQ5o94nABRvw3RxNVYipFh9U3s9Kb0Wyu6RJvGPMkv9yEc0C43jZ_hkerlaSpjOxymw3tVbMt_a0npJSsMQnPMYHYBqlbcDbBMwXVTwsWY5RThHqpdySmLe6fUOzStawmNh2VpCsqQsr8EKtllurN2RgkYdMNGWzuuoOy_g41U6QkNcfRVcPmuEWIomcMjmzpBLMT_0-M_MrS4yUrUK5NXaJ4QTQrOIlpSaiG4Zvh0qYP4kWDWn2GcgdrtdtPo6HudDAniMDmww4KvumE99f30utvwP9ui98_yT2-gvFdz6QL3VztcaFlaiuP1NZXdUw"; 
	
		//in the future, make a localCachedRestGet to a url
	}
	
	public String localCachedRestGet(String rest_Uri, final Map<String, ?> _data,  final Map<String, ?> _attr) throws ExecutionException {
		
		
		
		Map<String,Object> request = new HashMap<String,Object>();
		request.put("url", rest_Uri);
		request.put("data", _data);
		request.put("attr", _attr);
		
		CacheLoader<Map<String,Object>,String> loader = new CacheLoader<Map<String,Object>,String>(){
			@Override
			public String load(Map<String,Object> req) throws Exception  {
				
				String url = (String) req.get("url");
				@SuppressWarnings("unchecked")
				Map<String,?> data = (Map<String,?>) req.get("data");
				@SuppressWarnings("unchecked")
				Map<String,?> attr = (Map<String,?>) req.get("attr");
				
				ForwarderIfc forwarder = RestfulGetForwarder.instance(url);
				
				try{
					Schema schema = retrieveLatestAvroSchema();
					String res =  forwarder.forward(data, schema,attr);
					return res;
				}catch(Throwable e){
					throw new Exception(e);
				}
				
			}
		};

		LoadingCache<Map<String,Object>, String> cache = CacheBuilder.newBuilder().refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);

		return cache.get(request);	
	
	}
	
	public String remoteRestPost(String rest_Uri, Map<String, ?> map,  Map<String, ?> attr) {

		try {

			ForwarderIfc forwarder = RestfulPostForwarder.instance(rest_Uri);
			Schema schema = retrieveLatestAvroSchema();
			return forwarder.forward(map, schema, attr);
		} catch (Throwable e) {
			log.error(e,e);
			e.printStackTrace();
			return EXCEPTION+ExceptionUtils.getStackTrace(e);
		}
	}
	
	public String remoteElasticSearchPost (String rest_Uri, Map<String, ?> map,  Map<String, ?> attr) {

		try {

			ForwarderIfc forwarder = ElasticSearchPostForwarder.instance(rest_Uri);
			Schema schema = retrieveLatestAvroSchema();
			return forwarder.forward(map, schema, attr);
		} catch (Throwable e) {
			log.error(e,e);
			e.printStackTrace();
			return EXCEPTION+ExceptionUtils.getStackTrace(e);
		}
	}

	static Map<String, String> rulesTempDB = new HashMap<String, String>() {
		{
			put("default", "drools/RouteGenericMapDataRules_default.drl");
			put("kaniu", "drools/RouteGenericMapDataRules_kaniu.drl");
			put("yaima", "drools/RouteGenericMapDataRules_yaima.drl");
			put("oneid", "drools/RouteGenericMapDataRules_oneid.drl");
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
	
	public synchronized Object searchMap(String searchkey, Map<String,?>map){
	
		if(map==null||map.isEmpty())return null;
		
		Set<String> keyset = map.keySet();
		Object retObj = null;
		for(String key : keyset){
			Object o = map.get(key);
			if(key.equals(searchkey)){ retObj = o;break;}
			else if (o instanceof Map){
				o = searchMap(searchkey,(Map<String,?>)o);
				if(o!=null){retObj = o;break;}
			}else{
				retObj = null;
			}
		}
		return retObj;
	}
	
	public synchronized Object searchMapFirstSubKey(String searchkey, Map<String,?>map){
		
		if(map==null||map.isEmpty())return null;
		
		Set<String> keyset = map.keySet();
		Object retObj = null;
		for(String key : keyset){
			Object o = map.get(key);
			//System.out.println(key+" contains "+searchkey+" ?  "+key.contains(searchkey));
			if(key.contains(searchkey)){ retObj = o;break;}
			else if (o instanceof Map){
				o = searchMapFirstSubKey(searchkey,(Map<String,?>)o);
				if(o!=null){retObj = o;break;}
			}else{
				retObj = null;
			}
		}
		return retObj;
	}

}
