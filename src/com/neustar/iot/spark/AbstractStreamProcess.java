package com.neustar.iot.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.neustar.iot.spark.cache.StaticCacheManager;
import com.neustar.iot.spark.kafka.SimplePayloadAvroStandardizationStreamProcess;
import com.neustar.iot.spark.rules.RulesForwardWorker;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;

public abstract class AbstractStreamProcess implements Serializable{

	static final Logger log = Logger.getLogger(AbstractStreamProcess.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = -6060091345310773499L;

	protected static Properties streamProperties = null;
	protected static Properties producerProperties = null;
	protected static Properties consumerProperties = null;
	
	static{
		InputStream consumerprops = SimplePayloadAvroStandardizationStreamProcess.class.getClassLoader().getResourceAsStream("consumer.props");
		InputStream streamprops = SimplePayloadAvroStandardizationStreamProcess.class.getClassLoader().getResourceAsStream("streamprocess.props");
		InputStream producerprops = SimplePayloadAvroStandardizationStreamProcess.class.getClassLoader().getResourceAsStream("producer.props");
		
		consumerProperties = new Properties();
		streamProperties = new Properties();
		producerProperties = new Properties();
		try{
			consumerProperties.load(consumerprops);
			streamProperties.load(streamprops);
			
			if (streamProperties.getProperty("group.id") == null) {
				streamProperties.setProperty("group.id", "group-localtest");
			}

			
			producerProperties.load(producerprops);
			
		}catch(Exception e){
			log.error(e,e);
			e.printStackTrace();
		}
	}

	public synchronized Configuration createHDFSConfiguration() {

		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		return hadoopConfig;
	}

	public  synchronized void appendToHDFS(String dirPathStr, String data) throws IOException  {

		Path dir_path = new Path(dirPathStr);
		Configuration conf = createHDFSConfiguration();
		FileSystem fs = FileSystem.get(conf);

		FSDataOutputStream out = null;
		if (!fs.exists(dir_path)) {
			out = fs.create(dir_path,false);
			fs.setReplication(dir_path,  (short)1);
			
		}else{
			out = fs.append(dir_path);
		}
		out.writeUTF(data);
		out.flush();
		out.close();
		
		//fs.close();//do not close because of timeout issues.

	}
	
	public  synchronized void writeToHDFS(String pathStr, String filename, String data) throws IOException  {

		Path path = new Path(pathStr);
		Configuration conf = createHDFSConfiguration();
		FileSystem fs = FileSystem.get(conf);

		
		if (!fs.exists(path)) {
			fs.createNewFile(path);
			fs.setReplication(path,  (short)1);
		}

		fs = path.getFileSystem(conf);
		
		FSDataOutputStream out = fs.append(path);
		out.writeUTF(data);

		fs.close();

	}

	@Deprecated
	public synchronized Schema readSchemaFromHDFS(Schema.Parser parser,String uri) throws IOException{

		Configuration conf = createHDFSConfiguration();
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


	public synchronized Schema readSchemaFromWeb(URL schemaUrl) throws IOException{

		InputStream stream = schemaUrl.openStream();
		Schema schema = new Schema.Parser().parse(stream);

		return schema;
	}



	/**
	 * Added Guava caching
	 * */
	@Deprecated
	public synchronized Schema retrieveLatestAvroSchema(String avro_schema_hdfs_location) throws IOException, ExecutionException{
		
		LoadingCache<String,Schema> cache = null;
		if((cache = (LoadingCache<String,Schema>) StaticCacheManager.getCache(StaticCacheManager.CACHE_TYPE.HdfsSchemaCache))!=null){
			return cache.get(avro_schema_hdfs_location);
		}
		
		CacheLoader<String,Schema> loader = new CacheLoader<String,Schema>(){
			@Override
			public Schema load(String key) throws Exception {
				Schema.Parser parser = new Schema.Parser();
				return readSchemaFromHDFS(parser, key);
			}
		};

		cache = CacheBuilder.newBuilder().refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);
		
		StaticCacheManager.insertCache(StaticCacheManager.CACHE_TYPE.HdfsSchemaCache, cache);
		
		return cache.get(avro_schema_hdfs_location);		
	}


	public synchronized Schema retrieveLatestAvroSchema(URL avroWebUrl) throws IOException, ExecutionException{
		
		LoadingCache<URL,Schema> cache = null;
		if((cache = (LoadingCache<URL,Schema>) StaticCacheManager.getCache(StaticCacheManager.CACHE_TYPE.WebSchemaCache))!=null){
			return cache.get(avroWebUrl);
		}
		
		CacheLoader<URL,Schema> loader = new CacheLoader<URL,Schema>(){
			@Override
			public Schema load(URL key) throws Exception {
				//Schema.Parser parser = new Schema.Parser();
				return readSchemaFromWeb(key);
			}
		};

		cache = CacheBuilder.newBuilder().refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);

		StaticCacheManager.insertCache(StaticCacheManager.CACHE_TYPE.WebSchemaCache, cache);
		
		return cache.get(avroWebUrl);		
	}


	public synchronized Properties retrieveLocalPackageProperties(final String propfile) throws IOException, ExecutionException{
		
		LoadingCache<String, Properties> cache = null;
		if((cache = (LoadingCache<String, Properties>) StaticCacheManager.getCache(StaticCacheManager.CACHE_TYPE.PropertiesCache))!=null){
			return cache.get(propfile);
		}
		
		CacheLoader<String,Properties> loader = new CacheLoader<String,Properties>(){
			@Override
			public Properties load(String key) throws Exception {
				InputStream props = CacheLoader.class.getClassLoader().getResourceAsStream(propfile);
				Properties properties = new Properties();
				properties.load(props);

				return properties;
			}
		};

		cache = CacheBuilder.newBuilder().refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);

		StaticCacheManager.insertCache(StaticCacheManager.CACHE_TYPE.PropertiesCache, cache);

		return cache.get(propfile);		
	}

	@Deprecated
	public Map<String,?> parseAvroData(byte[] avrodata, String avro_schema_hdfs_location) throws Exception{
		Schema schema = retrieveLatestAvroSchema(avro_schema_hdfs_location);
		AvroParser<Map<String,?>> avroParser = new AvroParser<Map<String,?>>(schema);
		return avroParser.parse(avrodata, new HashMap<String,Object>());		
	}

	@Deprecated
	public String parseAvroData(byte[] avrodata, String avro_schema_hdfs_location, Class<String> type) throws Exception{
		Schema schema = retrieveLatestAvroSchema(avro_schema_hdfs_location);
		AvroParser<String> avroParser = new AvroParser<String>(schema);
		return avroParser.parse(avrodata, new String());		
	}


	public Map<String,?> parseAvroData(byte[] avrodata, URL avro_schema_web_url) throws Exception{
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url);
		AvroParser<Map<String,?>> avroParser = new AvroParser<Map<String,?>>(schema);
		return avroParser.parse(avrodata, new HashMap<String,Object>());		
	}

	public  String parseAvroData(byte[] avrodata, URL avro_schema_web_url, Class<String> type) throws Exception{
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url );
		AvroParser<String> avroParser = new AvroParser<String>(schema);
		return avroParser.parse(avrodata, new String());		
	}
	
	public synchronized  Map<String,?> parseJsonData(byte[] jsondata) throws Exception{
		ObjectMapper mapper = new ObjectMapper();
		Map<String,?> map =  mapper.readValue(jsondata, new TypeReference<Map<String, ?>>(){});

		return map;
	}
	
	public synchronized  String objectToJson(Object o) throws Exception{
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(o);

	}
	
	public GenericRecord createGenericRecord(Map<String,?> map, Schema schema) throws JsonGenerationException, JsonMappingException, IOException{

		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString( map);
		byte[] avro = AvroUtils.serializeJson(json, schema);
		GenericRecord record = AvroUtils.avroToJava(avro, schema);
		
		return record;
	}

	public void reportException(Map<String ,Object>data, Exception e){
		try{
			data.put("EXCEPTION", e.getMessage());
			data.put("EXCEPTION CAUSE", e.getCause().getMessage());
			//Schema schema = retrieveLatestAvroSchema(avro_schema_hdfs_location);
			//ForwarderIfc kafka = new KafkaForwarder();
			//kafka.forward(data, schema);
			new RulesForwardWorker().remoteElasticSearchPost("https://search-iotaselasticsearch-qtpuykpxgabuzfidzncsfyp7k4.us-west-2.es.amazonaws.com/ioteventindex/exceptions", data, null);
		}catch(Throwable e2){
			log.error(e, e);
		}
	}

	public Properties getStreamProperties() {
		return streamProperties;
	}

	public Properties getProducerProperties() {
		return producerProperties;
	}

	public Properties getConsumerProperties() {
		return consumerProperties;
	}



}
