package com.neustar.iot.spark.forward.mqtt;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.neustar.iot.spark.forward.ForwarderIfc;

import io.parser.avro.AvroUtils;

public class MQTTForwarder implements ForwarderIfc , MqttCallback {

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
	
	public MQTTForwarder(String _broker) throws SQLException, ClassNotFoundException {
		broker=_broker;		
	}
	
	public MQTTForwarder(String _broker, String _clientId) throws SQLException, ClassNotFoundException {
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
	public String forward(Map<String, ?> map, Schema schema, Map<String, ?> attr) throws Throwable {
		
		topic = (String) attr.get("topic");
		qos =attr.get("qos")==null?1:(Integer)attr.get("qos");
		clientId=attr.get("clientId")==null?clientId:(String) attr.get("clientId");	

		return forward(map,schema);
	}
	
	@Override
	public synchronized String forward(Map<String, ?> map, Schema schema) throws Throwable {

		String ret = "SUCCESS";
		try{	
			ObjectMapper mapper = new ObjectMapper();
			String json = mapper.writeValueAsString(map);
			//byte[] avrobytes = AvroUtils.serializeJava(map, schema);
			sendMessage(json.getBytes());
		}catch(Exception e){
			log.error(e,e);
			ret="EXCEPTION caused by "+e.getMessage();
		}
		return  ret;
	}

	private void sendMessage(byte[] message) throws MqttPersistenceException, MqttException, IOException{

		MqttMessage mqttmessage = new MqttMessage(message);
		mqttmessage.setQos(qos);
		clientConnection().publish(topic, mqttmessage);
		System.out.println("Message published");


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

	@Override
	public void connectionLost(Throwable arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public void subscribe(Map<String, ?> attr) throws Throwable {
		
		topic = (String) attr.get("topic");
		qos =attr.get("qos")==null?1:(Integer)attr.get("qos");
		clientId=attr.get("clientId")==null?clientId: (String)attr.get("clientId");	
		
		MqttMessage mqttmessage = new MqttMessage();
		mqttmessage.setQos(qos);
		clientConnection().subscribe(topic);
	}

	Set<byte[]>messageSet = new HashSet<byte[]>();
	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		
		byte[] payloadbytes = message.getPayload();
		messageSet.add(payloadbytes);
		
	}
	/***/
	public byte[] pollMessage(final int milliTimeout) throws InterruptedException, JsonGenerationException, JsonMappingException, IOException {
		int currentTimeout = milliTimeout;
		do{
			if(messageSet.isEmpty()){
				Thread.currentThread().sleep(1);
			}else{
				byte[]  message = messageSet.iterator().next();
				messageSet.remove(messageSet);
				return message;
			};
			
		}while(--currentTimeout!=0);
		
		return new ObjectMapper().writeValueAsBytes("{\"result\":\"Timeout exception - no results within timeout period ="+milliTimeout+"\"}");
	}
	
}
