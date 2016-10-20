package com.neustar.kafka.examples;

import com.google.common.io.Resources;
import com.neustar.iot.spark.kafka.BusinessProcessAvroConsumerStreamProcess;

import io.parser.avro.AvroUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class Producer extends Consumer {
	Properties properties = null;
	public Producer(){
		super();
		InputStream props = BusinessProcessAvroConsumerStreamProcess.class.getClassLoader().getResourceAsStream("producer.props");
		properties = new Properties();
		try {
			properties.load(props);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    public static void main(String[] args) throws IOException {
    	
    	new Producer().produce(args);
    }
    
    
    
    public void produce(String []args){
        // set up the producer
        //KafkaProducer<String, String> producer = null;
    	properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

            KafkaProducer<String, byte[]> producer = new KafkaProducer<String,byte[]>(properties);
      
            String topic =  properties.getProperty("topic.id");//"testexternaltopic";
        try {
        		Schema schema = this.retrieveLatestAvroSchema(avro_schema_hdfs_location);
        		byte[] avro = toAvro(args[0], "TELEMETRY", schema);
        		
                // send lots of messages
                producer.send(
                		new ProducerRecord<String, byte[]>(topic,avro));

                    producer.flush();
                    
                    System.out.println("Sent msg ");

        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }
    
    
    public static byte[] toAvro(String payload, String type, Schema schema) throws IOException{
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("sourceid", "yaima");
		mesg.put("payload", payload);
		mesg.put("messagetype", type);
		mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
		mesg.put("messageid", UUID.randomUUID()+"");
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		System.out.println(Bytes.toString(avro));
		
		return avro;
	}
    
    

}
