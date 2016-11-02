
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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

import com.thoughtworks.xstream.core.util.Base64Encoder;

import io.parser.avro.AvroUtils;

public class RegistryRESTVolumeTest {
	
	ExecutorService executor = null;
	{
		executor = Executors.newFixedThreadPool(10);
	}

	static Schema schema = null;
	static {
		try {
			schema = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/registry-to-spark/versions/current/remoterequest.avsc").openStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String args[]) {

		RegistryRESTVolumeTest producer = new RegistryRESTVolumeTest();

		try {

			for (int i = 0; i < 1; i++) {
				// send lots of messages
				producer.send(toAvro(
						"{\"owner\"=\"kaniu\", \"test\"=\"Testing the format of this internal json\"}"
						));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

	}

	public static URLConnection openConnection() throws IOException {
		URL url = new URL("http://ec2-52-41-165-85.us-west-2.compute.amazonaws.com:8988/JsonGatewayWebService/queue/avro/stream/topic/out.topic.registry?userid=default");
		//URL url = new URL("http://127.0.0.1:8988/JsonGatewayWebService/api/queue/avro/stream/topic/out.topic.registry?userid=default");
		
		URLConnection connection = url.openConnection();
		connection.setDoOutput(true);
		connection.setRequestProperty("Authentication",
				"jwt_abc");
		connection.setRequestProperty("API-KEY",
				"123");
		connection.setRequestProperty("Content-Type",MediaType.APPLICATION_OCTET_STREAM);
		connection.setConnectTimeout(10000);
		connection.setReadTimeout(10000);
		return connection;
	}

	public void send(byte[] message) {
		executor.submit(new Sender(message));
	}
	
	public static byte[] toAvro(String payload) throws IOException{
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("deviceId", "RaspiLightUUID-Demo");
		mesg.put("payload", "{\"value\":true}");
		mesg.put("txId",  "a37183ac-ba57-4213-a7f3-1c1608ded09e");
		mesg.put("path", "/a/light");
		mesg.put("verb", "dosomething");
		mesg.put("header", "hub-request");
		mesg.put("statusCode",0);
		//create avro
		byte[] avro = AvroUtils.serializeJava(mesg, schema);
		
		System.out.println(Bytes.toString(avro));
		System.out.println("Length  = "+avro.length);
		
		
		
		return avro;
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
				URLConnection connection = RegistryRESTVolumeTest.openConnection();

				OutputStream  out = connection.getOutputStream();
				
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
				//throw e;
			}
			return resposneBuilder.toString();
		}

	}
}
