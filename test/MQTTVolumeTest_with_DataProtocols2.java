
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import io.parser.avro.AvroUtils;

public class MQTTVolumeTest_with_DataProtocols2 {
	
	
	@Test
	public void printBytes() throws IOException{
		 byte[] bytes = toAvro("owners/143", "REGISTRY");
		  
		 System.out.print("[");
	        for (int i = 0; i < bytes.length; i++) {
	            System.out.print(bytes[i] & 0xff);
	            if(i!=bytes.length-1){
	            	System.out.print(",");
	            }
	        }
	        System.out.println("]");
	}
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

	public static void main(String args[]) throws IOException {

		MQTTVolumeTest_with_DataProtocols2 producer = new MQTTVolumeTest_with_DataProtocols2();
		
		try {
			int i;
			for ( i = 0; i < 1; i++) {
				// send lots of messages
				//producer.send(toAvro(String.format("{ \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i),"NOTIFICATION"));

				// every so often send to a different topic
				/*if (i % 2 == 0) {
					producer.send(toAvro(
							String.format("owners/143", System.nanoTime() * 1e-9, i),"REGISTRY"));
					producer.send(toAvro(
							String.format("owners/315", System.nanoTime() * 1e-9, i),"REGISTRY"));
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
		mesg.put("id", "kaniu");
		mesg.put("payload", payload);
		mesg.put("messagetype", type);
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		System.out.println(Bytes.toString(avro));
		
		return avro;
	}

	public static URLConnection openConnection() throws IOException {
		URL url = new URL("http://ec2-52-41-165-85.us-west-2.compute.amazonaws.com:8090/gateway/queues");
		URLConnection connection = url.openConnection();
		connection.setDoOutput(true);
		connection.setRequestProperty("Authorization",
				"Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJqb2huLmRvZUBnbWFpbC5jb20iLCJyb2xlIjoiVVNFUiJ9.bZwX6cFExrcHm8P9onE_wTAkJlEeb8Qz4J2e7vqQSADplc5o9lWurlKi-xOdPU_wm0QlWaGIeLwzTZUQ97EC1g");
		connection.setRequestProperty("Content-Type", "text/plain");
		connection.setConnectTimeout(5000);
		connection.setReadTimeout(5000);
		return connection;
	}

	public void send(byte[] message) {
		executor.submit(new Sender(message));
	}

	class Sender implements Callable<String> {
		byte[] message = null;

		public Sender(byte[] _message) {
			message = _message;
		}

		@Override
		public String call() throws Exception {
			StringBuilder resposneBuilder = new StringBuilder();
			try {
				URLConnection connection = MQTTVolumeTest_with_DataProtocols2.openConnection();
				//OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
				OutputStream out = connection.getOutputStream();
				try{
				out.write(message);
				}finally{
				out.close();
				}
				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

				String response = null;
				try{
				while ((response = in.readLine()) != null) {
					resposneBuilder.append(response).append(" ");
				}
				System.out.println("\nCrunchify REST Service Invoked Successfully..." + resposneBuilder);
				}finally{
				in.close();
				}
			} catch (Exception e) {
				System.out.println("\nError while calling Crunchify REST Service");
				e.printStackTrace();
			}
			return resposneBuilder.toString();
		}

	}
}
