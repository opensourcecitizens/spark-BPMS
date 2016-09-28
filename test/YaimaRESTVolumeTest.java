
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.core.MediaType;

public class YaimaRESTVolumeTest {
	
	ExecutorService executor = null;
	{
		executor = Executors.newFixedThreadPool(10);
	}

	public static void main(String args[]) {

		YaimaRESTVolumeTest producer = new YaimaRESTVolumeTest();

		try {

			for (int i = 0; i < 1; i++) {
				// send lots of messages
				producer.send(
						"{\"owner\"=\"kaniu\", \"test\"=\"Testing the format of this internal json\"}"
						);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

	}

	public static URLConnection openConnection() throws IOException {
		URL url = new URL("http://ec2-52-41-165-85.us-west-2.compute.amazonaws.com:8988/JsonGatewayWebService/api/queue/json/stream/topic/jsonexternaltopic?userid=yaima");
		//URL url = new URL("http://localhost:8988/JsonGatewayWebService/api/queue/topic/stream/sometopic");
		
		URLConnection connection = url.openConnection();
		connection.setDoOutput(true);
		connection.setRequestProperty("Authentication",
				"jwt_abc");
		connection.setRequestProperty("API-KEY",
				"123");
		connection.setRequestProperty("Content-Type", MediaType.APPLICATION_JSON);
		connection.setConnectTimeout(1000);
		connection.setReadTimeout(1000);
		return connection;
	}

	public void send(String message) {
		executor.submit(new Sender(message));
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
				URLConnection connection = YaimaRESTVolumeTest.openConnection();
				OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
				out.write(message);
				out.close();
				
				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

				String response = null;

				while ((response = in.readLine()) != null) {
					resposneBuilder.append(response).append(" ");
				}
				System.out.println("\n REST Service Invoked Successfully with response..." + resposneBuilder.toString());
				in.close();
			} catch (Exception e) {
				System.out.println("\nError while calling Crunchify REST Service");
				e.printStackTrace();
			}
			return resposneBuilder.toString();
		}

	}
}
