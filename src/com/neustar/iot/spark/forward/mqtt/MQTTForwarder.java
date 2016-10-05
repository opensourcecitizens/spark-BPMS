package com.neustar.iot.spark.forward.mqtt;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.neustar.iot.spark.forward.ForwarderIfc;

import io.parser.avro.AvroUtils;

public class MQTTForwarder implements ForwarderIfc {

	Logger log = Logger.getLogger(MQTTForwarder.class);
	private static final long serialVersionUID = 1L;

	MqttClient sampleClient = null;

	static String topic = "test/my/in";
	static int qos = 2;
	static String broker = "tcp://ec2-52-42-35-89.us-west-2.compute.amazonaws.com:1883";
	static String clientId = "JavaSample";

	public MQTTForwarder(String _broker, String _topic, int _qos, String _clientId) throws SQLException, ClassNotFoundException {
		topic = _topic;
		qos =_qos;
		broker=_broker;
		clientId=_clientId;		
	}

	private MQTTForwarder(){}

	private MqttClient clientConnection() throws IOException, MqttException {

		if(sampleClient==null ){
			MemoryPersistence persistence = new MemoryPersistence();
			sampleClient = new MqttClient(broker, clientId, persistence);
		}

		if(!sampleClient.isConnected()){
			MqttConnectOptions connOpts = new MqttConnectOptions();
			//connOpts.setCleanSession(true);
			System.out.println("Connecting to broker: " + broker);
			sampleClient.connect(connOpts);
			System.out.println("Connected");

		}


		return sampleClient;
	}


	@Override
	public synchronized String forward(Map<String, ?> map, Schema schema) throws Throwable {

		String ret = "SUCCESS";
		try{	
			ObjectMapper mapper = new ObjectMapper();
			String json = mapper.writeValueAsString(map);
			byte[] avrobytes = AvroUtils.serializeJson(json, schema);
			sendMessage(avrobytes);
		}catch(Exception e){
			log.error(e,e);
			ret="EXCEPTION caused by "+e.getMessage();
		}
		return  ret;
	}

	protected  Configuration createHDFSConfiguration() {

		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		return hadoopConfig;
	}

	private void sendMessage(byte[] message) throws MqttPersistenceException, MqttException, IOException{

		MqttMessage mqttmessage = new MqttMessage(message);
		mqttmessage.setQos(qos);
		clientConnection().publish(topic, mqttmessage);
		System.out.println("Message published");


	}

	@Override
	public String forward(Map<String, ?> map, Schema schema, Map<String, ?> attr) throws Throwable {
		// TODO Auto-generated method stub
		return forward(map,schema);
	}

	@Override
	public void finalize() throws Throwable{
		super.finalize();
		if(sampleClient!=null)
			try{
				sampleClient.disconnect();
			}finally{
				//nothing
			}
		System.out.println("Disconnected");

	}
}
