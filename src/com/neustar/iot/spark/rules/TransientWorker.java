package com.neustar.iot.spark.rules;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.neustar.iot.spark.forward.ForwarderIfc;
import com.neustar.iot.spark.forward.phoenix.PhoenixForwarder;
import com.neustar.iot.spark.forward.rest.RestfulGetForwarder;
import com.neustar.iot.spark.forward.rest.RestfulPutForwarder;
import com.neustar.iot.spark.kafka.SparkKafkaConsumer;

public class TransientWorker implements Serializable{

	private String phoenix_zk_JDBC = null;
	private String hdfs_output_dir = null;
	private String rest_Uri = null;
	private String avro_schema_hdfs_location = null;
	private Properties properties = null;
	
	public TransientWorker(){

	}
	
	protected void loadPropertiesFromDB(String uniqueId) throws IOException{
		InputStream props = SparkKafkaConsumer.class.getClassLoader().getResourceAsStream("consumer.props");
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
	
	protected Schema readSchemaFromHDFS(Schema.Parser parser,String uri) throws IOException{
		
		Configuration conf = new Configuration();
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
	
	protected Schema retrieveLatestAvroSchema() throws IOException{
		Schema.Parser parser = new Schema.Parser();
        Schema schema = readSchemaFromHDFS(parser, avro_schema_hdfs_location);//parser.parse(SimpleAvroProducer.USER_SCHEMA);
		return schema;
	}
	
	public void writeToDB(String someUniqueId, Map<String, ?> map) throws Throwable{
		
		loadPropertiesFromDB(someUniqueId);
		
		Schema schema = retrieveLatestAvroSchema();
		
		ForwarderIfc phoenixConn = PhoenixForwarder.singleton(phoenix_zk_JDBC);	
		phoenixConn.forward(map,schema);

	}
	
	public String remoteRestPut(String someUniqueId, Map<String, ?> map) {
		try{
		loadPropertiesFromDB(someUniqueId);
		
		ForwarderIfc forwarder = RestfulPutForwarder.singleton(rest_Uri);
		Schema schema = retrieveLatestAvroSchema();
		return forwarder.forward(map,schema);
		}catch(Throwable e){
			return e.getLocalizedMessage();
		}
	}
	
	public String remoteRestGet(String someUniqueId, Map<String, ?> map) {
		
		try{
		loadPropertiesFromDB(someUniqueId);
		
		ForwarderIfc forwarder = RestfulGetForwarder.singleton(rest_Uri);
		Schema schema = retrieveLatestAvroSchema();
		return forwarder.forward(map,schema);
		}catch(Throwable e){
			return e.getLocalizedMessage() ;
		}
	}
	

	

}
