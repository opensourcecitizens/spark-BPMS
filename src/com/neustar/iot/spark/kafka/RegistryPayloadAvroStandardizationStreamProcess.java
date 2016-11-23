package com.neustar.iot.spark.kafka;

import scala.Tuple2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import java.text.SimpleDateFormat;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
	private RegistryPayloadAvroStandardizationStreamProcess(){}
	
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
	    
	    //messages.repartition(numPartitions)
	    
	    JavaDStream<Map<String,?>> lines = messages.map(new Function<Tuple2<String, byte[]>, Map<String,?>>() {
			private static final long serialVersionUID = 1L;
			String daily_hdfsfilename = new SimpleDateFormat("yyyyMMdd").format(new Date());
			
		@Override
	      public Map<String,?> call(Tuple2<String, byte[]> tuple2) throws Exception {
				String parallelHash = Math.random()+"";
				log.debug("Raw data : Append to hdfs");
				System.out.println("Raw data : Append to hdfs");
				appendToHDFS(hdfs_output_dir +"/"+APP_NAME+"/RAW/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".txt", System.nanoTime() +" | "+  tuple2._1+" |"+ tuple2._2+"\n");

				Map<String , ?> genericPayload = parseAvroData(tuple2._2, registry_avro_schema_web_url);

				appendToHDFS(hdfs_output_dir +"/"+APP_NAME+"/JSON/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".json", parseAvroData(tuple2._2, registry_avro_schema_web_url, String.class)+"\n");

			return genericPayload ;
	      }
	    });

	    //System.out.println(lines.count());
	    //lines = lines.repartition(6);
	    
	    
	    lines.foreachRDD(new Function<JavaRDD<Map<String,?>>, Void>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<Map<String,?>> stringJavaRDD) throws Exception {

				final Properties props = producerProperties;
				stringJavaRDD.foreachAsync(new VoidFunction<Map<String,?>>() {

							private static final long serialVersionUID = 1L;
							
							@Override
							public void call(Map<String,?> msg) throws Exception {
								
								
								try{
									
									log.debug("Security check");
									System.out.println("Security check");
									if(securityCheck(msg)){
										System.out.println("Writing message: "+msg);
										Future<RecordMetadata>  res = createAndSendAvroToQueue(msg,props);
										System.out.println("Wrote to topic:"+res.get().topic()+";  partition:"+res.get().partition());
										log.info("Wrote to topic:"+res.get().topic()+";  partition:"+res.get().partition());
										//String res = createAndSendAvroToQueue(msg,props);
										//System.out.println(res);
										//log.info(res);
									}else{
										//create error message
										
									}
									
								
								}catch( Throwable e){
									//check error and decide if to recycle msg if parser error.
									log.error(e,e);
									e.printStackTrace();
									throw e;
								}
									
							}



						}

				);
				
				return null;
			}});


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
	


	protected synchronized  Future<RecordMetadata> createAndSendAvroToQueue(Map<String,?> payloadMap, Properties props) throws IOException, ExecutionException {
		
		GenericRecord payload = createGenericRecord( payloadMap, retrieveLatestAvroSchema(registry_avro_schema_web_url));
		//create generic avro record 
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url);

		GenericRecord outMap = new GenericData.Record(schema);
		String sourceid = "default";
		String time = new DateTime ( DateTimeZone.UTC ).toString( );
		String msgid = UUID.randomUUID()+sourceid;//fromString( sourceid+time ).toString();
		
		outMap.put("messageid", msgid);
		outMap.put("sourceid", sourceid);
		outMap.put("registrypayload", payload);
		outMap.put("payload", "");
		outMap.put("createdate", time);
		outMap.put("messagetype", "REGISTRY_RESPONSE");
		
		//create avro
		byte[] avro = AvroUtils.serializeJava(outMap, schema);
		@SuppressWarnings("resource")
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
		Future<RecordMetadata> response = producer.send(new ProducerRecord<String, byte[]>(props.getProperty("topic.id"), avro));
		return response;
		//KafkaProducerClient<byte[]> kafka = (KafkaProducerClient<byte[]>) KafkaProducerClient.singleton();
		//String ret = kafka.send(avro, props.getProperty("topic.id"));
		//return ret;
	}


}
