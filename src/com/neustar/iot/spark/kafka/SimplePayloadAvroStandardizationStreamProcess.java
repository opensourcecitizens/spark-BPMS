package com.neustar.iot.spark.kafka;

import scala.Tuple2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.Charsets;
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
public final class SimplePayloadAvroStandardizationStreamProcess extends AbstractStreamProcess{
	static final Logger log = Logger.getLogger(SimplePayloadAvroStandardizationStreamProcess.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String inputTopics = null;
	private String outputTopic = null;
	
	private int numThreads;
	private String hdfs_output_dir = null;
	private URL avro_schema_web_url = null; 
	private static String APP_NAME="SimplePayloadAvroStreamProcess"; 
	
	//private Properties properties = null;
	//private Properties producerProperties = null;
	
	public SimplePayloadAvroStandardizationStreamProcess(String _topics, int _numThreads, String _outTopic) throws IOException {
		inputTopics=_topics;
		numThreads=_numThreads;
		outputTopic=_outTopic;
		
		hdfs_output_dir = streamProperties.getProperty("hdfs.outputdir");
		avro_schema_web_url = streamProperties.getProperty("avro.schema.web.url")!=null?new URL(streamProperties.getProperty("avro.schema.web.url")):null;
		
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.err.println("Usage: "+APP_NAME+" <inputTopics> <numThreads> <outputTopic>");
			System.exit(1);
		}
			
		int numThreads = Integer.parseInt(args[1]);
		
		new SimplePayloadAvroStandardizationStreamProcess(args[0], numThreads, args[2]).run();
	}
	
	
	
	public void run() throws IOException{
		
		//StreamingExamples.setStreamingLogLevels();
		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.driver.allowMultipleContexts","true");
		
		// Create the context with 1000 milliseconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));
		
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
	      public Map<String,?> call(Tuple2<String, byte[]> tuple2) throws IOException, ClassNotFoundException, SQLException {
				String parallelHash = Math.random()+"";
				log.debug("Raw data : Append to hdfs");
				appendToHDFS(hdfs_output_dir +"/"+APP_NAME+"/RAW/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".txt", System.nanoTime() +" | "+  tuple2._1+" |"+ tuple2._2+"\n");

				//parse - json 
				Map<String, Object> data = new HashMap<String, Object>();
				data.put("payload", new String(tuple2._2,Charsets.UTF_8 ));
				data.put("messagetype", "REGISTRY_POST");
				data.put("sourceid", "bogdan");
				
				/*
				 try {
					data = parseJsonData(tuple2._2);
					log.debug("Parsed data : Append to hdfs");
					appendToHDFS(hdfs_output_dir +"/"+APP_NAME+"/PARSED/_MSG_" + daily_hdfsfilename +"/"+parallelHash+ ".txt", System.nanoTime() +" | "+  tuple2._2+" | "+  data);


				} catch (Exception e) {
					//check error and decide if to recycle msg if parser error.
					e.printStackTrace();
					log.error(e,e);
				}
				*/
				
			return data ;
	      }
	    });

	    //System.out.println(lines.count());
	    lines = lines.repartition(6);
	    
	    
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
									
									if(securityCheck(msg)){
										Future<RecordMetadata>  res = createAndSendAvroToQueue(msg,props);
										System.out.println("Wrote to topic:"+res.get().topic()+";  partition:"+res.get().partition());
										log.info("Wrote to topic:"+res.get().topic()+";  partition:"+res.get().partition());
									}else{
										//create error message
										
									}
									
								
								}catch( Throwable e){
									//check error and decide if to recycle msg if parser error.
									log.error(e,e);
									e.printStackTrace();
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
	protected synchronized  boolean securityCheck(Map<String, ?> msg) {
		
		String authentication = (String) msg.get("authentication");
		String sourceid = (String) msg.get("sourceid");
		
		return true;
	}
	

	protected synchronized  Future<RecordMetadata> createAndSendAvroToQueue(Map<String, ?> msg, Properties props) throws IOException, ExecutionException {
		
		//create generic avro record 
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url);

		GenericRecordBuilder outMap =  new GenericRecordBuilder(schema);
		String sourceid = (String) msg.get("sourceid");
		String time = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date());
		String msgid = UUID.randomUUID()+sourceid;//fromString( sourceid+time ).toString();
		
		outMap.set("messageid", msgid);
		outMap.set("sourceid", sourceid);
		outMap.set("payload", msg.get("payload"));
		outMap.set("createdate", time);
		outMap.set("messagetype", msg.get("messagetype"));
		

		//create avro
		byte[] avro = AvroUtils.serializeJava(outMap.build(), schema);
		
		//send avro to end process
		//String message = new String(avro);
		//KafkaProducerClient kafka = new KafkaProducerClient();
				//Message message = new Message(avro);
		//kafka.send(message, outputTopic);

		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
		Future<RecordMetadata> response = producer.send(new ProducerRecord<String, byte[]>(outputTopic, avro));
		
		
		return response;
	}


}
