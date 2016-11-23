package com.neustar.iot.spark.kafka;

import scala.Tuple2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.Base64;
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
 * Reads from a topic args[]
 * Checks message source and packages json/message as avro payload.
 */
public final class SecurityAndAvroStandardizationStreamProcess extends AbstractStreamProcess{
	static final Logger log = Logger.getLogger(SecurityAndAvroStandardizationStreamProcess.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String inputTopics = null;
	//private String outputTopic = null;
	
	private int numThreads;
	private String hdfs_output_dir = null;
	//private String avro_schema_hdfs_location = null;
	private URL avro_schema_web_url = null;
	private URL registry_avro_schema_web_url = null;
	private static String APP_NAME="Json2AvroStreamProcess"; 
	enum APP_TYPE{ Json2Avro, DeviceJson2Avro , OneIdJson2Avro };
	
	@SuppressWarnings("unused")
	private SecurityAndAvroStandardizationStreamProcess(){}
	
	public SecurityAndAvroStandardizationStreamProcess(String _inTopics, int _numThreads, String _outTopic) throws IOException {
		inputTopics=_inTopics;
		numThreads=_numThreads;
		//outputTopic=_outTopic;
		

		producerProperties.setProperty("topic.id", _outTopic);
		producerProperties.setProperty("currentTopic", inputTopics);
		hdfs_output_dir = streamProperties.getProperty("hdfs.outputdir");
		avro_schema_web_url = streamProperties.getProperty("avro.schema.web.url")!=null?new URL(streamProperties.getProperty("avro.schema.web.url")):null;
		registry_avro_schema_web_url = streamProperties.getProperty("registry.avro.schema.web.url")!=null?new URL(streamProperties.getProperty("registry.avro.schema.web.url")):null;
		
	}

	public static void main(String[] args) throws IOException {
		
		if (args.length < 3) {
			System.err.println("Usage: "+APP_NAME+" <topics> <numThreads> <outputTopic>");
			
			System.exit(1);
		}
		APP_NAME = APP_NAME+"_"+args[0];	
		int numThreads = Integer.parseInt(args[1]);
		
		new SecurityAndAvroStandardizationStreamProcess( args[0], numThreads, args[2] ).run();
	}
	
	
	
	public void run() throws IOException{
		
		//StreamingExamples.setStreamingLogLevels();
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
	      public Map<String,?> call(Tuple2<String, byte[]> tuple2) throws IOException, ClassNotFoundException, SQLException {
				String parallelHash = Math.random()+"";
				log.debug("Raw data : Append to hdfs");
				appendToHDFS(hdfs_output_dir +"/"+APP_NAME+"/RAW/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".txt", System.nanoTime() +" | "+  tuple2._1+" |"+ tuple2._2+"\n");

				//parse - json 
				Map<String, ?> data = null;
				try {
					data = parseJsonData(tuple2._2);
					log.debug("Parsed data : Append to hdfs");
					appendToHDFS(hdfs_output_dir +"/"+APP_NAME+"/PARSED/_MSG_" + daily_hdfsfilename +"/" + parallelHash+ ".txt", System.nanoTime() +" | "+  tuple2._2+" | "+  data+"\n");


				} catch (Exception e) {
					//check error and decide if to recycle msg if parser error.
					e.printStackTrace();
					log.error(e,e);
				}
				
			return data ;
	      }
	    });

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
	

	
	/**
	 * To be used for verifying userid vs securitytoken
	 * */
	protected synchronized  boolean securityCheck(Map<String, ?> msg) {
		
		//String authentication = (String) msg.get("authentication");
		//String sourceid = (String) msg.get("sourceid");
		
		return true;
	}
	
	public synchronized  Future<RecordMetadata> createAndSendAvroToQueue(Map<String, ?> jsonData, Properties props) throws IOException, ExecutionException {
		
		Schema schema = retrieveLatestAvroSchema(avro_schema_web_url);
		String sourceid = null;
		String msgid = null;
		//GenericRecordBuilder outMap =  new GenericRecordBuilder(schema);
		GenericRecord outMap = new GenericData.Record(schema);
		
		String time = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date());
		outMap.put("createdate", time);
		
		
		Schema schema_remoteReq = retrieveLatestAvroSchema(registry_avro_schema_web_url);
		
		if(props.getProperty("currentTopic").startsWith("device.event")){
			sourceid = (String) (jsonData.get("sourceid")==null?"default":jsonData.get("sourceid"));
			
			GenericRecord remotemesg = temporaryCreateRemoteMessage(jsonData, schema_remoteReq);
						
			outMap.put("sourceid", sourceid);	
			msgid = UUID.randomUUID()+sourceid;
			outMap.put("messageid", msgid);	
			
			outMap = this.deviceEvent(outMap, remotemesg);
			
		}else if (props.getProperty("currentTopic").startsWith("device.out")){
			 sourceid = (String) (jsonData.get("sourceid")==null?"default":jsonData.get("sourceid"));			
			 GenericRecord remotemesg = temporaryCreateRemoteMessage(jsonData, schema_remoteReq);
			 outMap.put("sourceid", sourceid);	
			 msgid = UUID.randomUUID()+sourceid;
			 outMap.put("messageid", msgid);
			 outMap = this.resolveDeviceState(outMap, remotemesg);
				
		}else if(props.getProperty("currentTopic").startsWith("device.onboard")){
			 sourceid = (String) (jsonData.get("sourceid")==null?"default":jsonData.get("sourceid"));
			 GenericRecord remotemesg = temporaryCreateRemoteMessage(jsonData, schema_remoteReq, true);
			 outMap.put("sourceid", sourceid);	
			 msgid = UUID.randomUUID()+sourceid;
			 outMap.put("messageid", msgid);
			 outMap = this.onboardDeviceState(outMap, remotemesg);	
		}else if(props.getProperty("currentTopic").startsWith("in.topic.oneid")){
			sourceid = "oneid";
			outMap.put("sourceid", sourceid);	
			msgid = UUID.randomUUID()+sourceid;
			outMap.put("messageid", msgid);
			outMap.put("registrypayload", null);
			outMap.put("payload", jsonData.get("payload"));
			outMap.put("messagetype", StringUtils.isNoneEmpty(jsonData.get("messagetype").toString())?jsonData.get("messagetype"):"TELEMETRY");
			
		}else{
			sourceid = (String) (jsonData.get("sourceid")==null?"default":jsonData.get("sourceid"));
			outMap.put("sourceid", sourceid);	
			msgid = UUID.randomUUID()+sourceid;
			outMap.put("messageid", msgid);
			outMap.put("registrypayload", null);
			outMap.put("payload", jsonData.get("payload"));
			outMap.put("messagetype", StringUtils.isNoneEmpty(jsonData.get("messagetype").toString())?jsonData.get("messagetype"):"TELEMETRY");

		}
		
		//outMap.build().
		//create avro
		byte[] avro = AvroUtils.serializeJava(outMap, schema);

		@SuppressWarnings("resource")
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
		Future<RecordMetadata> response = producer.send(new ProducerRecord<String, byte[]>(props.getProperty("topic.id"), avro));
		
		return response;
	}
	
	private GenericRecord temporaryCreateRemoteMessage(Map<String,?> jsonData, Schema schema , boolean ... decodePayload){
		GenericRecord record = new GenericData.Record(schema);
		//GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		
		///cleanup because schema is not imposed
		record.put("path", jsonData.get("path")==null?"":jsonData.get("path"));
		record.put("verb", jsonData.get("verb")==null?"":jsonData.get("verb"));
		record.put("statusCode", jsonData.get("statusCode")==null?0:jsonData.get("statusCode"));
		try {
			
			if(decodePayload!=null && decodePayload.length>0 && decodePayload[0]==true){
				record.put("payload", jsonData.get("payload")==null?"":new String(Base64.decodeBase64((String)jsonData.get("payload"))));
			}else
				record.put("payload", jsonData.get("payload")==null?"":objectToJson(jsonData.get("payload")));
		} catch (Exception e) {
			record.put("payload",e.getCause());
		}

		record.put("txId", jsonData.get("txId")==null?"":jsonData.get("txId"));
		record.put("deviceId", jsonData.get("deviceId")==null?"":jsonData.get("deviceId"));
		record.put("header", jsonData.get("header")==null?"":jsonData.get("header"));
		
		return record;
	}
	
	
	private GenericRecord temporaryCreateRemoteMessageForOnboard(Map<String,?> jsonData, Schema schema , boolean ... decodePayload){
		GenericRecord record = new GenericData.Record(schema);
		//GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		
		///cleanup because schema is not imposed
		record.put("path", jsonData.get("path")==null?"":jsonData.get("path"));
		record.put("verb", jsonData.get("verb")==null?"":jsonData.get("verb"));
		record.put("statusCode", jsonData.get("statusCode")==null?0:jsonData.get("statusCode"));
		try {
			//new String( Base64.decodeBase64(mapper.writeValueAsString(bytes)))
			System.out.println(new String(Base64.decodeBase64((String)jsonData.get("payload"))));
			record.put("payload", jsonData.get("payload")==null?"":new String(Base64.decodeBase64((String)jsonData.get("payload"))));
					} catch (Exception e) {
			record.put("payload",e.getCause());
		}

		record.put("txId", jsonData.get("txId")==null?"":jsonData.get("txId"));
		record.put("deviceId", jsonData.get("deviceId")==null?"":jsonData.get("deviceId"));
		record.put("header", jsonData.get("header")==null?"":jsonData.get("header"));
		///done cleanup
		//byte[] avro = AvroUtils.serializeJava(jsonData, schema_remoteReq);
		//GenericRecord remotemesg = AvroUtils.avroToJava(avro, schema_remoteReq);
		return record;
	}
	
	public GenericRecord resolveDeviceState(GenericRecord curRecord,GenericRecord remotemesg){
		
		curRecord.put("registrypayload", remotemesg);
		
		curRecord.put("payload", null);
		
		curRecord.put("messagetype", "REGISTRY_POST");
		
		return curRecord;
	}
	
	public GenericRecord onboardDeviceState(GenericRecord curRecord,GenericRecord remotemesg){
		
		curRecord.put("registrypayload", remotemesg);
		
		curRecord.put("payload", null);
		
		curRecord.put("messagetype", "DEVICE_ONBOARDING");
		
		return curRecord;
	}
	public GenericRecord deviceEvent(GenericRecord curRecord,GenericRecord remotemesg){
		
		curRecord.put("registrypayload", remotemesg);
		
		curRecord.put("payload", null);
		
		curRecord.put("messagetype", "REGISTRY_PUT");
		
		return curRecord;
	}


}
