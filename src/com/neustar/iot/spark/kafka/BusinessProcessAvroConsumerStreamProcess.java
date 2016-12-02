package com.neustar.iot.spark.kafka;

import scala.Tuple2;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.neustar.iot.spark.AbstractStreamProcess;
import com.neustar.io.net.forward.ForwarderIfc;
import com.neustar.io.net.forward.phoenix.PhoenixForwarder;
import com.neustar.io.net.forward.rest.RestfulGetForwarder;
import com.neustar.iot.spark.rules.RulesProxy;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
	

	private String hdfs_output_dir = null;
	private String avro_schema_hdfs_location = null;
	private URL avro_schema_web_url = null;
	private static String APP_NAME ="BusinessProcessStreamProcessor";
	
	public BusinessProcessAvroConsumerStreamProcess(String _topics, int _numThreads) throws IOException {
		topics_str=_topics;
		numThreads=_numThreads;
						
		hdfs_output_dir = streamProperties.getProperty("hdfs.outputdir");
		avro_schema_hdfs_location = streamProperties.getProperty("avro.schema.hdfs.location");
		avro_schema_web_url = streamProperties.getProperty("avro.schema.web.url")!=null?new URL(streamProperties.getProperty("avro.schema.web.url")):null;

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
		
		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.driver.allowMultipleContexts","true");
		
		// Create the context with 200 milliseconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(200));
		
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = topics_str.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		
		 // Create direct kafka stream with brokers and topics
		 Set<String> topicsSet = new HashSet<>(Arrays.asList(topics_str.split(",")));
		 Map<String, String> kafkaParams = new HashMap<>();
		    kafkaParams.put("metadata.broker.list", consumerProperties.getProperty("bootstrap.servers"));
		    
	    JavaPairInputDStream<String, byte[]> messages = KafkaUtils.createDirectStream(
	        jssc,
	        String.class,
	        byte[].class,
	        StringDecoder.class,
	        DefaultDecoder.class,
	        kafkaParams,
	        topicsSet
	    );
	    
	    
		messages.foreachRDD(new Function<JavaPairRDD<String, byte[]>, Void>() {
			private static final long serialVersionUID = 1L;
			String daily_hdfsfilename = new SimpleDateFormat("yyyyMMdd").format(new Date());

			@Override
			public Void call(JavaPairRDD<String, byte[]> rdd) throws IOException, ClassNotFoundException, SQLException {

				rdd.foreachPartition( new VoidFunction<Iterator<Tuple2<String, byte[]>>>(){
					
					final Properties props = producerProperties;
					//final RulesProxy rulesProxy = RulesProxy.instance();
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, byte[]>> itTuple) throws Exception {
						
						while(itTuple.hasNext()){

							Tuple2<String, byte[]> tuple2  = itTuple.next();					

							String parallelHash = Math.random()+"";

							Map<String, Object> data = null;
							

								try {
									//appendToHDFS(hdfs_output_dir  +"/"+APP_NAME+ "/RAW/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".txt", System.nanoTime() +" | "+  tuple2._2+"\n");
									//parse - 

									data = (Map<String, Object>) parseAvroData(tuple2._2,avro_schema_web_url);
									//log.debug("Parsed data : Append to hdfs");
									//appendToHDFS(hdfs_output_dir  +"/"+APP_NAME+"/JSON/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".json",  parseAvroData(tuple2._2, avro_schema_web_url, String.class)+"\n");

									
									log.debug("Apply rules");
									applyRules(data);
									
									//rulesProxy.executeRules(data);
									
								} catch (Exception e) {
									log.error(e,e);
									//check error and decide if to recycle msg if parser error.
									//e.printStackTrace();
									
									reportException(data,e);
									
								}

	

						}

					}

				});	

				return null;
			}
		});

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
	
	protected void applyRules(Map<String, ?> msg) throws Exception{
		RulesProxy.instance().executeRules(msg);
	}
	
	@Deprecated
	protected void writeToDB(Map<String, ?> map, String phoenix_zk_JDBC) throws Throwable{
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url);
		
		ForwarderIfc phoenixConn = PhoenixForwarder.singleton(phoenix_zk_JDBC);	
		phoenixConn.forward(map,schema);
	}


	
	/**Use forwarders instead**/
	@Deprecated
	protected String remoteRest(Map<String, ?> map, String uri) throws Throwable{
		ForwarderIfc forwarder = RestfulGetForwarder.singleton(uri);
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url);
		return forwarder.forward(map,schema);
	}

	


}
