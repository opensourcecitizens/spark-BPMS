package com.neustar.iot.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.neustar.iot.spark.cache.PooledResourceCacheManager;
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
	
	
	
	public FileSystem acquireFS() throws IOException{
		FileSystem  fs_  = null;
		if(fs_==null){
			Configuration conf = createHDFSConfiguration();
			fs_ = FileSystem.get(conf);
		}
		return fs_;
	}

	public synchronized Configuration createHDFSConfiguration() {

		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		return hadoopConfig;
	}

	@Deprecated
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
	
	public  synchronized void appendToHDFS(String dirPathStr, String data,FileSystem fs) throws IOException  {

		Path dir_path = new Path(dirPathStr);

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
	
	@Deprecated
	public  synchronized void writeToHDFS(String pathStr, String filename, String data) throws IOException  {

		Path path = new Path(pathStr);
		Configuration conf = createHDFSConfiguration();
		FileSystem fs = FileSystem.get(conf);

		//FileSystem fs = acquireFS();
		
		if (!fs.exists(path)) {
			fs.createNewFile(path);
			fs.setReplication(path,  (short)1);
		}

		fs = path.getFileSystem(conf);
		
		FSDataOutputStream out = fs.append(path);
		out.writeUTF(data);

		fs.close();

	}
	
	public  synchronized void writeToHDFS(String pathStr, String filename, String data, FileSystem fs) throws IOException  {

		Path path = new Path(pathStr);
		
		//FileSystem fs = acquireFS();
		
		if (!fs.exists(path)) {
			fs.createNewFile(path);
			fs.setReplication(path,  (short)1);
		}
		
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
	
	/**
	 * Note: this is not a pooled resource?
	 * */
	public synchronized KafkaProducer<String, byte[]> retrieveCachedKafkaProducer(Properties kafkaProps) throws IOException, ExecutionException{
		
		LoadingCache<Properties,KafkaProducer<String, byte[]>> cache = null;
		
		if((cache = (LoadingCache<Properties, KafkaProducer<String, byte[]>>) StaticCacheManager.getCache(StaticCacheManager.CACHE_TYPE.KafkaProducerCache))!=null){
			return cache.get(kafkaProps);
		}
		
		CacheLoader<Properties,KafkaProducer<String, byte[]>> loader = new CacheLoader<Properties,KafkaProducer<String, byte[]>>(){
			@Override
			public KafkaProducer<String, byte[]> load(Properties key) throws Exception {
				
				KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(key);
				
				return producer;
			}
		};
		
		RemovalListener<Properties,KafkaProducer<String, byte[]>> removalListener = new RemovalListener<Properties,KafkaProducer<String, byte[]>>() {
			  public void onRemoval(RemovalNotification<Properties,KafkaProducer<String, byte[]>> removal) {
				  KafkaProducer<String, byte[]> conn = removal.getValue();
				  conn.close(); 
			  }

			};

		cache = CacheBuilder.newBuilder().maximumSize(100).refreshAfterWrite((long)1, TimeUnit.HOURS).removalListener(removalListener).build(loader);

		StaticCacheManager.insertCache(StaticCacheManager.CACHE_TYPE.KafkaProducerCache, cache);
		
		return cache.get(kafkaProps);		
	}
	
	static final int KAFKA_PRODUCER_POOL_SIZE = 20;
	
	public synchronized KafkaProducer<String, byte[]> retrieveCachedPooledKafkaProducer(Properties kafkaProps) throws IOException, ExecutionException{
		
		
		LoadingCache<Properties,ConcurrentLinkedQueue<KafkaProducer<String, byte[]>>> cache = null;
		
		if( PooledResourceCacheManager.getCache(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache) !=null){
			if(!PooledResourceCacheManager.getCachedPool(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache, kafkaProps).isEmpty()) {
				return (KafkaProducer) PooledResourceCacheManager.pollCachedPool(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache, kafkaProps);
			}
		}
		
		CacheLoader<Properties,ConcurrentLinkedQueue<KafkaProducer<String, byte[]>>> loader = new CacheLoader<Properties,ConcurrentLinkedQueue<KafkaProducer<String, byte[]>>>(){
			@Override
			public ConcurrentLinkedQueue<KafkaProducer<String, byte[]>> load(Properties key) throws Exception {
				ConcurrentLinkedQueue<KafkaProducer<String, byte[]>> queue = new ConcurrentLinkedQueue<KafkaProducer<String, byte[]>>();
				
				for(int i = 0 ; i < KAFKA_PRODUCER_POOL_SIZE;i++){
					KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(key);
				
					queue.add(producer);

				}
				
				return queue;
			}
		};
		
		RemovalListener<Properties,ConcurrentLinkedQueue<KafkaProducer<String, byte[]>>> removalListener = new RemovalListener<Properties,ConcurrentLinkedQueue<KafkaProducer<String, byte[]>>>() {
			  public void onRemoval(RemovalNotification<Properties,ConcurrentLinkedQueue<KafkaProducer<String, byte[]>>> removal) {
				  ConcurrentLinkedQueue<KafkaProducer<String, byte[]>> queue = removal.getValue();
				  while(!queue.isEmpty()){
				  KafkaProducer<String, byte[]> conn = queue.poll();
				  conn.close(); 
				  }
			  }

			};
			
			//removalListener(removalListener)

		cache = CacheBuilder.newBuilder().maximumSize(100).refreshAfterWrite((long)1, TimeUnit.MINUTES).removalListener(removalListener).build(loader);

		PooledResourceCacheManager.insertCache(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache, cache);
		
		return (KafkaProducer) PooledResourceCacheManager.pollCachedPool(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache, kafkaProps);
	
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
	
	public  synchronized Map<String,? extends Object> parseJsonData(byte[] jsondata) throws Exception{
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
			data.put("EXCEPTION TRACE", e);
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
