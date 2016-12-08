
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.MediaType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.util.Bytes;

import io.parser.avro.AvroUtils;

public class OneIdRESTVolumeTest {
	
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
	public static void main(String args[]) throws InterruptedException {

		OneIdRESTVolumeTest producer = new OneIdRESTVolumeTest();

		try {

			for (int i = 0; i < 1; i++) {
				// send lots of messages
				producer.send("{\"owner\":\"oneid\", \"test\":\"Testing the format of this internal json\"}");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println(producer.executor.awaitTermination(10, TimeUnit.SECONDS));
			producer.executor.shutdown();
		}
		
		

	}

	public static HttpURLConnection openConnection() throws IOException {
		//URL url = new URL("http://ec2-52-41-165-85.us-west-2.compute.amazonaws.com:8988/JsonGatewayWebService/api/queue/json/stream/topic/in.topic.oneid?userid=oneid");
		//URL url = new URL("http://localhost:8988/JsonGatewayWebService/api/queue/json/stream/topic/out.topic.registry?userid=default");
		URL url = new URL("http://localhost:8988/JsonGatewayWebService/api/OneIdRESTService/queue/json/stream/");
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setDoOutput(true);
		connection.addRequestProperty("Authorization",
				"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6Ik1UVTRRVFZEUTBFMFJVSXhSRU5HTWpCR01FRTBPRGMwUTBRMU5VUkVOelV4T1VKRE1UQkZOQSJ9.eyJyb2xlIjoiVVNFUl9ST0xFIiwiaWQiOjI5NCwiaXNzIjoiaHR0cHM6Ly9iYWdkaS5hdXRoMC5jb20vIiwic3ViIjoiYXV0aDB8NTgyYzhmYWVmZTc2NzZlZTE4NTY1OGExIiwiYXVkIjoiWjhSTFVtdUdlbjNlbmVRek0yaXlxcFVFS0VEWVowUTUiLCJleHAiOjE0NzkzMzYzNzQsImlhdCI6MTQ3OTMzMjc3NH0.R3KYmSEfvsL3vIZiPU9vuPRM3hSK8zDMcbzGJjZbR_YMkYCxf1FUgiP5fvkHprsC22mRokz0wow6ola7CjtH4tOLj1s1g8xzWchSr47MQ6Sgplf-mB7P1EMuHh7c0I9weR4OGd-smqMMxgBXVwv4Sn_1-4tDTQDLs11VsuPVwuyFQd-TD4BJkPm5tyrW5rpjbduFj1N6h2ZI62CHLVEhCP17YtzDAyu0XkdqLdPf8fGCNDYQhGBs2GvQljrubnmsevxG0WI7qo5wrLP36SbAGihYJm_Ivv3gdjpyCYaAvPo5dISIUTcynG7coha4nXTKiJ8yW-w18rL8pmQNc4mH1Q");
		connection.setRequestProperty("API-KEY",
				"123");
		connection.addRequestProperty("Content-Type",MediaType.APPLICATION_JSON);
		
		connection.setConnectTimeout(10000);
		connection.setReadTimeout(10000);
		return connection;
	}

	public Future<String> send(String message) {
		return executor.submit(new Sender(message));
	}

	class Sender implements Callable<String> {
		String message = null;

		public Sender(String _message) {
			message = _message;
		}

		@Override
		public String call() throws Exception {
			StringBuilder resposneBuilder = new StringBuilder();
			try {
				URLConnection connection = OneIdRESTVolumeTest.openConnection();
				
				//System.out.println(connection.getHeaderField("Authentication"));
				//System.out.println(connection.getHeaderFields());
				
				OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());

				
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
