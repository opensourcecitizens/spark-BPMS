
import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Test;

import com.neustar.iot.spark.forward.mqtt.MQTTForwarder;

import io.parser.avro.AvroUtils;

public class MQTTVolumeTest_with_DataProtocols {
	ExecutorService executor = null;
	static Schema schema = null;
	static {
		try {
			schema = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/versions/current/NeustarMessage.avsc").openStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	{
		executor = Executors.newFixedThreadPool(10);
		
	}

	
	@Test
	public void singleMessage() throws Exception{
		 byte[] bytes = toAvro("owners/143", "REGISTRY");
		 Sender sender = new Sender(bytes);
		 String ret = sender.call();
		 
		 System.out.println(ret);
		 
	}
	
	@Test
	public void sendLargeMessage() throws IOException, InterruptedException, ExecutionException{
		MQTTVolumeTest_with_DataProtocols producer = new MQTTVolumeTest_with_DataProtocols();
		
		 byte[] bytes = toAvro(
					"mqtt just testing a sentence with Maya's Monster Inc. Lamp Annd a very long sentence that makes this message even bigger for testing payload capacity","TELEMETRY");

	     Future<String> ret =   producer.send(bytes);
	        
	     System.out.println(ret.get());

	}

	
	
	@Test
	public void testForwarder() throws Throwable{
		 
		 GenericRecord mesg = new GenericData.Record(schema);		
			mesg.put("id", "kaniu");
			mesg.put("payload", "owners/143");
			mesg.put("messagetype", "RESPONSE");
			ObjectMapper mapper = new ObjectMapper();
		Map<String,?> data = 	mapper.readValue(mesg.toString(), new TypeReference<Map<String, ?>>(){});	
			
		String topic = "test/my/in";
		int qos = 2;
		String broker = "tcp://ec2-52-42-35-89.us-west-2.compute.amazonaws.com:1883";
		String clientId = "JavaSample";
			
		MQTTForwarder forwarder = new MQTTForwarder(broker,  topic,  qos,  clientId);
		String ret = forwarder.forward(data, schema);
		
		System.out.println(ret);
		 
	}
	

	public static void main(String args[]) throws IOException {

		MQTTVolumeTest_with_DataProtocols producer = new MQTTVolumeTest_with_DataProtocols();
		
		try {
			int i;
			for ( i = 0; i < 1; i++) {
				// send lots of messages
				//producer.send(toAvro(String.format("{ \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i),"NOTIFICATION"));

				// every so often send to a different topic
				/*
				if (i % 2 == 0) {
					producer.send(toAvro(
							String.format("owners/143", System.nanoTime() * 1e-9, i),"TELEMETRY"));
					producer.send(toAvro(
							String.format("owners/315", System.nanoTime() * 1e-9, i),"TELEMETRY"));
					producer.send(toAvro(
							String.format("\"t\":%.3f, \"k\":%d TEST EXCEPTION! ", System.nanoTime() * 1e-9, i),"EXCEPTION"));

					System.out.println("Sent msg number " + i);
				}
				*/
				 byte[] bytes = toAvro(
							"mqtt just testing a sentence with Maya's Monster Inc. Lamp Annd a very long sentence that makes this message even bigger for testing payload capacity","TELEMETRY");

			        producer.send(bytes);
			}
			
			System.out.println("Total sent " + i);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
		
		producer.close();

	}
	
	private void close() {
		executor.shutdown();
	}
	


	private static byte[] toAvro(String payload, String type) throws IOException{
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("sourceid", "kaniu");
		mesg.put("payload", payload);
		mesg.put("messagetype", type);
		mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
		mesg.put("messageid", UUID.randomUUID()+"");
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		//System.out.println(Bytes.toString(avro));
		
		return avro;
	}

	static String topic = "test/my/in";
	static int qos = 2;
	static String broker = "tcp://ec2-52-42-35-89.us-west-2.compute.amazonaws.com:1883";
	static String clientId = "JavaSample";

	static MqttClient sampleClient = null;
	public static synchronized MqttClient  clientConnection() throws IOException, MqttException {
		
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
	public void finalize() throws Throwable{
		super.finalize();
		 sampleClient.disconnect();
         System.out.println("Disconnected");
         
	}

	public Future<String> send(byte[] message) {
		return executor.submit(new Sender(message));
	}

	class Sender implements Callable<String> , MqttCallback {
		
		byte[] message = null;

		public Sender(byte[] _message) {
			message = _message;
		}

		@Override
		public String call() throws Exception {

			try {
				
				MqttClient sampleClient =  MQTTVolumeTest_with_DataProtocols.clientConnection();
				MqttMessage mqttmessage = new MqttMessage(message);
				mqttmessage.setQos(qos);
	            sampleClient.publish(topic, mqttmessage);
	            System.out.println("Message published");

				
			} catch (Exception e) {
				System.out.println("\nError while calling Crunchify REST Service");
				e.printStackTrace();
			}
			return "success";
		}
		
		public void connectionLost(Throwable arg0) {
			System.err.println("connection lost");
		}
		@Override	
		public void deliveryComplete(IMqttDeliveryToken arg0) {
			System.err.println("delivery complete");
		}
		@Override		
		public void messageArrived(String topic, MqttMessage message) throws Exception {
			System.out.println("topic: " + topic);
			System.out.println("message: " + new String(message.getPayload()));
		}

		

	}
}
