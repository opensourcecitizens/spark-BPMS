package com.neustar.iot.spark.kafka;

import scala.Tuple2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.neustar.iot.spark.AbstractStreamProcess;

import io.parser.avro.AvroUtils;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import java.io.IOException;
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
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Is a Kafka consumer using Spark api. 
 * Reads from a topic specified in
 * producer.props. Writes to 3 outputs: HBase,HDFS, Rest
 */
public final class RegistryPayloadAvroStandardizationStreamProcess extends AbstractStreamProcess{
	static final Logger log = Logger.getLogger(RegistryPayloadAvroStandardizationStreamProcess.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String inputTopics = null;
	private String outputTopic = null;
	
	private int numThreads;
	private String hdfs_output_dir = null;
	private URL avro_schema_web_url = null; 
	private URL registry_avro_schema_web_url = null;

	private static String APP_NAME="RegistryPayloadAvroStreamProcess"; 

	@SuppressWarnings("unused")
	public RegistryPayloadAvroStandardizationStreamProcess(){}
	
	public RegistryPayloadAvroStandardizationStreamProcess(String _topics, int _numThreads, String _outTopic) throws IOException {
		inputTopics=_topics;
		numThreads=_numThreads;
		outputTopic=_outTopic;

		producerProperties.setProperty("topic.id", outputTopic);
		hdfs_output_dir = streamProperties.getProperty("hdfs.outputdir");
		avro_schema_web_url = streamProperties.getProperty("avro.schema.web.url")!=null?new URL(streamProperties.getProperty("avro.schema.web.url")):null;
		registry_avro_schema_web_url=streamProperties.getProperty("registry.avro.schema.web.url")!=null?new URL(streamProperties.getProperty("registry.avro.schema.web.url")):null;
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.err.println("Usage: "+APP_NAME+" <inputTopics> <numThreads> <outputTopic>");
			System.exit(1);
		}
			
		int numThreads = Integer.parseInt(args[1]);
		
		new RegistryPayloadAvroStandardizationStreamProcess(args[0], numThreads, args[2]).run();
	}
	
	
	
	public void run() throws IOException{
		
		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.driver.allowMultipleContexts","true");
		
		// Create the context with 500 milliseconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(500));
		
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = inputTopics.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		
		 // Create direct kafka stream with brokers and topics
		 Set<String> topicsSet = new HashSet<>(Arrays.asList(inputTopics.split(",")));
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

				rdd.foreachPartitionAsync( new VoidFunction<Iterator<Tuple2<String, byte[]>>>(){
					
					final Properties props = producerProperties;
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, byte[]>> itTuple) throws Exception {

						FileSystem fs = acquireFS();
						
						while(itTuple.hasNext()){

							Tuple2<String, byte[]> tuple2  = itTuple.next();					

							String parallelHash = Math.random()+"";

							Map<String, ? extends Object> data = null;
							try {

								appendToHDFS(hdfs_output_dir +"/"+APP_NAME+"/RAW/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".txt", System.nanoTime() +" | "+  tuple2._1+" |"+ tuple2._2+"\n",fs);

								//data = parseAvroData(tuple2._2, registry_avro_schema_web_url);

								//appendToHDFS(hdfs_output_dir +"/"+APP_NAME+"/JSON/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".json", parseAvroData(tuple2._2, registry_avro_schema_web_url, String.class)+"\n",fs);

								data = parseJsonData(tuple2._2);

								appendToHDFS(hdfs_output_dir +"/"+APP_NAME+"/JSON/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".json", objectToJson(data)+"\n",fs);

								
								log.debug("Security check");

								if(securityCheck(data)){
									Future<RecordMetadata>  res = createAndSendAvroToQueue(data,props);
									//System.out.println("Wrote to topic:"+res.get().topic()+";  partition:"+res.get().partition());
									log.debug("Wrote to topic:"+res.get().topic()+";  partition:"+res.get().partition());
								}else{
									//create error message

								}

							} catch (Exception e) {
								//check error and decide if to recycle msg if parser error.
								
								log.error(e,e);
								reportException((Map<String, Object>) data,e);
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
	
	public synchronized  Map<String,?> parseJsonData(byte[] jsondata) throws Exception{
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue(jsondata, new TypeReference<Map<String, ?>>(){});
	}
	
	/**
	 * To be used for verifying userid vs securitytoken
	 * */
	protected synchronized  boolean securityCheck(Map<String,?> msg) {
		
		//String authentication = (String) msg.get("authentication");
		//String sourceid = (String) msg.get("sourceid");
		
		return true;
	}
	


	

	protected synchronized  Future<RecordMetadata> createAndSendAvroToQueue(Map<String,?> payloadMap, Properties props) throws Exception {
		
		String jsonpayload = objectToJson(payloadMap);
		
		//GenericRecord payload = createGenericRecord( payloadMap, retrieveLatestAvroSchema(registry_avro_schema_web_url));
		//create generic avro record 
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url);

		GenericRecord outMap = new GenericData.Record(schema);
		String sourceid = "default";
		String time = new DateTime ( DateTimeZone.UTC ).toString( );
		String msgid = UUID.randomUUID()+sourceid;//fromString( sourceid+time ).toString();
		
		outMap.put("messageid", msgid);
		outMap.put("sourceid", sourceid);
		outMap.put("registrypayload", null);
		outMap.put("payload", jsonpayload);
		outMap.put("createdate", time);
		outMap.put("messagetype", "REGISTRY_RESPONSE");
		
		//create avro
		byte[] avro = AvroUtils.serializeJava(outMap, schema);
		
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
		Future<RecordMetadata> response = producer.send(new ProducerRecord<String, byte[]>(props.getProperty("topic.id"), avro));
		producer.close();
		
		return response;
	}


}
