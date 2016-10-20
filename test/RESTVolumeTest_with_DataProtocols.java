
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.core.MediaType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import io.parser.avro.AvroUtils;

public class RESTVolumeTest_with_DataProtocols {
	
	
	@Test
	public void sendLargeMessage() throws IOException{
		RESTVolumeTest_with_DataProtocols producer = new RESTVolumeTest_with_DataProtocols();
		
		 byte[] bytes = toAvro(
					"kn 1 just testing a sentence with Maya's Monster Inc. Lamp And a very long sentence that makes this message even bigger for testing payload capacity","TELEMETRY");

	        producer.send(bytes);
	        
	        //close();
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

		RESTVolumeTest_with_DataProtocols producer = new RESTVolumeTest_with_DataProtocols();
		
		try {
			int i;
			for ( i = 0; i < 10; i++) {
				// send lots of messages
				//producer.send(toAvro(String.format("{ \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i),"NOTIFICATION"));

				// every so often send to a different topic
				/*if (i % 2 == 0) {
					producer.send(toAvro(
							String.format("owners/143", System.nanoTime() * 1e-9, i),"TELEMETRY"));
			
				}else 
					*/
					byte[] bytes = toAvro(
							"{\"owner\"=\"kaniu\", \"test\"=\"Testing the format of this internal json\"}","TELEMETRY");

				 
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

	public static byte[] toAvro(String payload, String type) throws IOException{
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("sourceid", "kaniu");
		mesg.put("payload", payload);
		mesg.put("messagetype", type);
		mesg.put("createdate",  DateFormat.getDateTimeInstance().format(new Date())+"");
		mesg.put("messageid", UUID.randomUUID()+"");
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		System.out.println(Bytes.toString(avro));
		
		return avro;
	}

	public static URLConnection openConnection() throws IOException {
		URL url = new URL("http://ec2-52-41-165-85.us-west-2.compute.amazonaws.com:8091/gateway/queues");
		URLConnection connection = url.openConnection();
		connection.setDoOutput(true);
		connection.setRequestProperty("Authorization",
				"Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJqb2huLmRvZUBnbWFpbC5jb20iLCJyb2xlIjoiVVNFUiJ9.bZwX6cFExrcHm8P9onE_wTAkJlEeb8Qz4J2e7vqQSADplc5o9lWurlKi-xOdPU_wm0QlWaGIeLwzTZUQ97EC1g");
		connection.setRequestProperty("Content-Type", MediaType.APPLICATION_OCTET_STREAM);
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
				URLConnection connection = RESTVolumeTest_with_DataProtocols.openConnection();
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
