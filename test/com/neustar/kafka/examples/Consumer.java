package com.neustar.kafka.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.neustar.iot.spark.AbstractStreamProcess;
import com.neustar.iot.spark.kafka.BusinessProcessAvroConsumerStreamProcess;

import jline.internal.Log;

import org.HdrHistogram.Histogram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * This program reads messages from two topics. Messages on "fast-messages" are
 * analyzed to estimate latency (assuming clock synchronization between producer
 * and consumer).
 * <p/>
 * Whenever a message is received on "slow-messages", the stats are dumped.
 */
public class Consumer extends AbstractStreamProcess {
	Logger log = Logger.getLogger(Consumer.class.getName());
	//static String FSOUTPUTFILE = "hdfs://ip-172-31-33-198.us-west-2.compute.internal:8020/testdata/CosumerMessages";
	//static String FSOUTPUTFILE_OTHER = "hdfs://ip-172-31-33-198.us-west-2.compute.internal:8020/testdata/UntrackedMessages";
	String avro_schema_hdfs_location = null;
	private Properties properties = null;
	public Consumer(){
		InputStream props = BusinessProcessAvroConsumerStreamProcess.class.getClassLoader().getResourceAsStream("consumer.props");
		properties = new Properties();
		try {
			properties.load(props);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		avro_schema_hdfs_location = properties.getProperty("avro.schema.hdfs.location");
	}
	public static void main(String[] args) throws Exception {
		
		new Consumer().stream();
	}
	
	
	public void stream() throws IOException  {
		// set up house-keeping
		//ObjectMapper mapper = new ObjectMapper();
		Histogram stats = new Histogram(1, 10000000, 2);
		Histogram global = new Histogram(1, 10000000, 2);

		// and the consumer
		// KafkaConsumer<String, String> consumer = null;
		InputStream props = Resources.getResource("consumer.props").openStream();
		Properties properties = new Properties();
		properties.load(props);
		if (properties.getProperty("group.id") == null) {
			properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
		}
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		//key.serializer=org.apache.kafka.common.serialization.StringSerializer
		//value.serializer=org.apache.kafka.common.serialization.StringSerializer
		
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(properties);

		consumer.subscribe(Arrays.asList("testexternaltopic"));
		int timeouts = 0;
		// noinspection InfiniteLoopStatement
		while (true) {
			// read records with a short timeout. If we time out, we don't
			// really care.
			ConsumerRecords<String, byte[]> records = consumer.poll(200);
			if (records.count() == 0) {
				timeouts++;
			} else {
				System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
				timeouts = 0;
			}
			for (ConsumerRecord<String, byte[]> record : records) {
				switch (record.topic()) {
				case "testexternaltopic":
					// the send time is encoded inside the message
					//long latency = -1;
					
					String msg;
					try {
						msg = this.parseAvroData(record.value(), avro_schema_hdfs_location, String.class);
						System.out.println(msg);
					} catch (Exception e) {
						log.error(e,e);
						e.printStackTrace();
					}
					
					
				default:
					throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
				}

			}

		}

	}

	private static Configuration createConfiguration() {

		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		return hadoopConfig;
	}

}
