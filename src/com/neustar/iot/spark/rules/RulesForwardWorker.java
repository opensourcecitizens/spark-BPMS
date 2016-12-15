package com.neustar.iot.spark.rules;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import com.neustar.iot.spark.cache.StaticCacheManager;
import com.neustar.io.net.forward.ForwarderIfc;
import com.neustar.io.net.forward.mqtt.MQTTForwarder;
import com.neustar.io.net.forward.phoenix.PhoenixForwarder;
import com.neustar.io.net.forward.rest.ElasticSearchPostForwarder;
import com.neustar.io.net.forward.rest.RestfulGetForwarder;
import com.neustar.io.net.forward.rest.RestfulPostForwarder;
import com.neustar.io.net.forward.rest.RestfulPutForwarder;

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
	private String phoenix_zk_jdbc_uri = null;
	private String rules_table_name = null;


	public RulesForwardWorker() {
		try {
			loadPropertiesFromDB();
		} catch (IOException e) {
			log.error(e);
		}
	}

	protected void loadPropertiesFromDB() throws IOException {

		user_rules_hdfs_location = streamProperties.getProperty("rules.hdfs.location");
		avro_schema_web_url = streamProperties.getProperty("avro.schema.web.url")!=null?new URL(streamProperties.getProperty("avro.schema.web.url")):null;
		registry_avro_schema_web_url = streamProperties.getProperty("registry.avro.schema.web.url")!=null?new URL(streamProperties.getProperty("registry.avro.schema.web.url")):null;
		phoenix_zk_jdbc_uri = streamProperties.getProperty("phoenix.zk.jdbc");
		rules_table_name = streamProperties.getProperty("rules.tablename");
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
			ForwarderIfc phoenixConn = getCachedDBForwarder(phoenix_zk_JDBC,tablename);//PhoenixForwarder.instance(phoenix_zk_JDBC,tablename);
			Schema schema = retrieveLatestAvroSchema();
			return phoenixConn.forward(map, schema, attr);
		} catch (Throwable e) {
			log.error(e);
			return EXCEPTION+e.getMessage();
		}

	}
	
	private ForwarderIfc getCachedDBForwarder(String phoenix_zk_JDBC,String tablename) throws ExecutionException{
		
		Map<String,Object> request = new HashMap<String,Object>();
		request.put("jdbcurl", phoenix_zk_JDBC);
		request.put("table", tablename);

		
		LoadingCache<Map<?,?>, ForwarderIfc> restGetCache = null;
		
		if((restGetCache = (LoadingCache<Map<?,?>, ForwarderIfc>) StaticCacheManager.getCache(StaticCacheManager.CACHE_TYPE.PhoenixForwarderCache))!=null){
			return restGetCache.get(request);
		}


		CacheLoader<Map<?,?>,ForwarderIfc> loader = new CacheLoader<Map<?,?>,ForwarderIfc>(){
			@Override
			public ForwarderIfc load(Map<?,?> req) throws Exception  {

				String jdbc = (String) req.get("jdbcurl");
				
				String table = (String) req.get("table");


				ForwarderIfc forwarder = PhoenixForwarder.instance(jdbc,table);

				return forwarder;

			}
		};

		restGetCache = CacheBuilder.newBuilder().maximumSize(10).refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);
		
		StaticCacheManager.insertCache(StaticCacheManager.CACHE_TYPE.PhoenixForwarderCache, restGetCache);
		
		return restGetCache.get(request);	

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
		} catch (Throwable e) {
			log.error(e);
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

	public String localCachedRestGet(String rest_Uri, final Map<String, ?> _data,  final Map<String, ?> _attr) throws ExecutionException {



		Map<String,Object> request = new HashMap<String,Object>();
		request.put("url", rest_Uri);
		request.put("data", _data);
		request.put("attr", _attr);
		
		LoadingCache<Map<?,?>, String> restGetCache = null;
		
		if((restGetCache = (LoadingCache<Map<?,?>, String>) StaticCacheManager.getCache(StaticCacheManager.CACHE_TYPE.RestGetCache))!=null){
			return restGetCache.get(request);
		}


		CacheLoader<Map<?,?>,String> loader = new CacheLoader<Map<?,?>,String>(){
			@Override
			public String load(Map<?,?> req) throws Exception  {

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

		restGetCache = CacheBuilder.newBuilder().maximumSize(10).refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);
		
		StaticCacheManager.insertCache(StaticCacheManager.CACHE_TYPE.RestGetCache, restGetCache);
		
		return restGetCache.get(request);	

	}


	public String localCachedRestPost(String rest_Uri, final Map<String, ?> _data,  final Map<String, ?> _attr) throws ExecutionException {

		Map<String,Object> request = new HashMap<String,Object>();
		request.put("url", rest_Uri);
		request.put("data", _data);
		request.put("attr", _attr);
		
		LoadingCache<Map<?,?>, String> cache = null;
		
		if((cache = (LoadingCache<Map<?,?>, String>) StaticCacheManager.getCache(StaticCacheManager.CACHE_TYPE.RestPostCache))!=null){
			return cache.get(request);
		}

		CacheLoader<Map<?,?>,String> loader = new CacheLoader<Map<?,?>,String>(){
			@Override
			public String load(Map<?,?> req) throws Exception  {

				String url = (String) req.get("url");
				@SuppressWarnings("unchecked")
				Map<String,?> data = (Map<String,?>) req.get("data");
				@SuppressWarnings("unchecked")
				Map<String,?> attr = (Map<String,?>) req.get("attr");

				ForwarderIfc forwarder = RestfulPostForwarder.instance(url);

				try{
					Schema schema = retrieveLatestAvroSchema();
					String res =  forwarder.forward(data, schema,attr);
					return res;
				}catch(Throwable e){
					throw new Exception(e);
				}

			}
		};

		cache = CacheBuilder.newBuilder().refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);

		StaticCacheManager.insertCache(StaticCacheManager.CACHE_TYPE.RestPostCache, cache);
		
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

	public String queryDBForRuleURI(String uniqueId) throws ClassNotFoundException, SQLException{
		PhoenixForwarder phoenix = PhoenixForwarder.instance(phoenix_zk_jdbc_uri,rules_table_name);
		PreparedStatement statement = phoenix.getConn().prepareCall("SELECT FILE_URI FROM "+rules_table_name+" WHERE SOURCEID like '"+uniqueId+"'");
		ResultSet result = statement.executeQuery();

		String fileUri = null;
		while(result.next()){
			fileUri = result.getString("FILE_URI");
		}


		return fileUri;
	}
	
	@Deprecated
	public InputStream retrieveRulesFromHDFS(String uniqueId) throws IOException {

		uniqueId = rulesTempDB.containsKey(uniqueId) ? uniqueId : "default";

		String uri = user_rules_hdfs_location+"/"+rulesTempDB.get(uniqueId);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		return fs.open(new Path(uri));
	}
	
	public InputStream retrieveRulesFromHDFS(String uniqueId,FileSystem fs) throws IOException {

		uniqueId = rulesTempDB.containsKey(uniqueId) ? uniqueId : "default";

		String uri = user_rules_hdfs_location+"/"+rulesTempDB.get(uniqueId);

		return fs.open(new Path(uri));
	}

	public Object searchJson(String searchKey, String jsonStr){
		Map<String, ?> map;
		try {
			map = super.parseJsonData(jsonStr.getBytes());
		} catch (Exception e) {
			log.error(e,e);
			return "Error retrieving key: '"+searchKey+"' in '"+jsonStr+"' ; cause by : "+e.getMessage();
		}
		return searchMap(searchKey, map);
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
