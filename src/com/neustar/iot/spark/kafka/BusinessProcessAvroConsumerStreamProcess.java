package com.neustar.iot.spark.kafka;

import scala.Tuple2;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.neustar.iot.spark.AbstractStreamProcess;
import com.neustar.iot.spark.forward.ForwarderIfc;
import com.neustar.iot.spark.forward.kafka.KafkaForwarder;
import com.neustar.iot.spark.forward.phoenix.PhoenixForwarder;
import com.neustar.iot.spark.forward.rest.RestfulGetForwarder;
import com.neustar.iot.spark.rules.RulesForwardWorker;
import com.neustar.iot.spark.rules.RulesProxy;

import io.parser.avro.AvroParser;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Is a Kafka consumer using Spark api. 
 * Reads from a topic specified in
 * producer.props. Writes to 3 outputs: HBase,HDFS, Rest
 */
public final class BusinessProcessAvroConsumerStreamProcess extends AbstractStreamProcess{
	static final Logger log = Logger.getLogger(BusinessProcessAvroConsumerStreamProcess.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String topics_str = null;
	private int numThreads;
	private String phoenix_zk_JDBC = null;
	private String hdfs_output_dir = null;
	private String rest_Uri = null;
	private String avro_schema_hdfs_location = null;
	private Properties properties = null;
	
	public BusinessProcessAvroConsumerStreamProcess(String _topics, int _numThreads) throws IOException {
		topics_str=_topics;
		numThreads=_numThreads;
		
		InputStream props = BusinessProcessAvroConsumerStreamProcess.class.getClassLoader().getResourceAsStream("consumer.props");
		properties = new Properties();
		properties.load(props);

		if (properties.getProperty("group.id") == null) {
			properties.setProperty("group.id", "group-localtest");
		}
		
		phoenix_zk_JDBC = properties.getProperty("phoenix.zk.jdbc");
		hdfs_output_dir = properties.getProperty("hdfs.outputdir");
		avro_schema_hdfs_location = properties.getProperty("avro.schema.hdfs.location");
		rest_Uri = properties.getProperty("rest.Uri");
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println("Usage: SparkKafkaConsumer <topics> <numThreads>");
			System.exit(1);
		}
			
		int numThreads = Integer.parseInt(args[1]);
		
		new BusinessProcessAvroConsumerStreamProcess(args[0], numThreads).run();
	}
	
	
	
	public void run() throws IOException{
		
		//StreamingExamples.setStreamingLogLevels();
		SparkConf sparkConf = new SparkConf().setAppName("SparkConsumer").set("spark.driver.allowMultipleContexts","true");
		
		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = topics_str.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		
		 // Create direct kafka stream with brokers and topics
		 Set<String> topicsSet = new HashSet<>(Arrays.asList(topics_str.split(",")));
		 Map<String, String> kafkaParams = new HashMap<>();
		    kafkaParams.put("metadata.broker.list", properties.getProperty("bootstrap.servers"));
		    
	    JavaPairInputDStream<String, byte[]> messages = KafkaUtils.createDirectStream(
	        jssc,
	        String.class,
	        byte[].class,
	        StringDecoder.class,
	        DefaultDecoder.class,
	        kafkaParams,
	        topicsSet
	    );
	    
	    //messages.repartition(numPartitions)
	    
	    JavaDStream<Map<String,?>> lines = messages.map(new Function<Tuple2<String, byte[]>, Map<String,?>>() {
			private static final long serialVersionUID = 1L;
			String daily_hdfsfilename = new SimpleDateFormat("yyyyMMdd").format(new Date());
			
		@Override
	      public Map<String,?> call(Tuple2<String, byte[]> tuple2) throws IOException, ClassNotFoundException, SQLException {
				String parallelHash = Math.random()+"";
				
				log.debug("Raw data : Append to hdfs: key="+tuple2._1);
				log.debug("Raw data : Append to hdfs: value="+String.valueOf(tuple2._2));
				
				System.out.println("Raw data : Append to hdfs: key="+tuple2._1);
				System.out.println("Raw data : Append to hdfs: value="+String.valueOf(tuple2._2));
				System.out.print("[");	
				for(int i = 0 ; i < tuple2._2.length; i++){
					System.out.print(tuple2._2[i]+",");	
				}
				System.out.print("]");	System.out.println("");	
				
				appendToHDFS(hdfs_output_dir + "/RAW/_MSG_" + daily_hdfsfilename +"/"+ parallelHash+ ".txt", System.nanoTime() +" | "+  tuple2._2);

				//parse - 
				Map<String, Object> data = null;
				try {
					
					
					data = (Map<String, Object>) parseAvroData(tuple2._2,avro_schema_hdfs_location);
					log.debug("Parsed data : Append to hdfs");
					appendToHDFS(hdfs_output_dir + "/JSON/_MSG_" + daily_hdfsfilename +"/"+ parallelHash+ ".json",  parseAvroData(tuple2._2, avro_schema_hdfs_location, String.class));

				} catch (Exception e) {
					log.error(e,e);
					//check error and decide if to recycle msg if parser error.
					e.printStackTrace();
					
					reportException(data,e);
					
				}
				
			return data ;
	      }
	    });

	    //System.out.println(lines.count());
	   lines = lines.repartition(6);
	    
	    
	    lines.foreachRDD(new Function<JavaRDD<Map<String,?>>, Void>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<Map<String,?>> stringJavaRDD) throws Exception {

				stringJavaRDD.foreachAsync(new VoidFunction<Map<String,?>>() {

							private static final long serialVersionUID = 1L;
							
							@Override
							public void call(Map<String,?> msg) throws Exception {
								
								
								try{
									//Map<String,?> msg = null;
									//while(it.hasNext() && (msg =it.next() ) !=null ){
									//apply rules here to determine what messages proceed to next level
									//you may also add tags for other rules to process downstream for re-routing etc
									log.debug("Apply rules");
									applyRules(msg);
								
									
									
								
								}catch( Throwable e){
									//check error and decide if to recycle msg if parser error.
									log.error(e);
								}
									
							}

						}

				);
				
				return null;
			}});


		jssc.start();
		jssc.awaitTermination();
		
	}
	

	

	

	@Deprecated
	protected Schema retrieveLatestAvroSchema_OLD() throws IOException, ExecutionException{
		Schema schema = null;
		Schema.Parser parser = new Schema.Parser();
        schema = readSchemaFromHDFS(parser, avro_schema_hdfs_location);//parser.parse(SimpleAvroProducer.USER_SCHEMA);
		return schema;
	}
	
	protected void applyRules(Map<String, ?> msg) throws Throwable{
		RulesProxy.instance().executeRules(msg);
	}
	
	
	protected void writeToDB(Map<String, ?> map, String phoenix_zk_JDBC) throws Throwable{
		Schema schema = retrieveLatestAvroSchema(avro_schema_hdfs_location);
		
		ForwarderIfc phoenixConn = PhoenixForwarder.singleton(phoenix_zk_JDBC);	
		phoenixConn.forward(map,schema);
	}

	protected  boolean writeToHDFS(String pathStr, String data) throws IOException {

		Path path = new Path(pathStr);
		FileSystem fs = FileSystem.get(createHDFSConfiguration());

		if (!fs.exists(path)) {
			fs.create(path);
		}

		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));

		br.write(data);
		br.close();

		return true;
	}
	
	/**Use forwarders instead**/
	@Deprecated
	protected String remoteRest(Map<String, ?> map, String uri) throws Throwable{
		ForwarderIfc forwarder = RestfulGetForwarder.singleton(uri);
		Schema schema = retrieveLatestAvroSchema(avro_schema_hdfs_location);
		return forwarder.forward(map,schema);
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

}
