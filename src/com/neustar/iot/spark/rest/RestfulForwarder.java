package com.neustar.iot.spark.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;


public class RestfulForwarder {
	
	private URLConnection connection = null;
	
	public URLConnection getConn() throws IOException{
		if( connection==null ){
		URL url = new URL("http://ec2-52-38-19-146.us-west-2.compute.amazonaws.com:8090/gateway/queues");
		connection = url.openConnection();
		connection.setDoOutput(true);
		connection.setRequestProperty("Authorization",
				"Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJqb2huLmRvZUBnbWFpbC5jb20iLCJyb2xlIjoiVVNFUiJ9.bZwX6cFExrcHm8P9onE_wTAkJlEeb8Qz4J2e7vqQSADplc5o9lWurlKi-xOdPU_wm0QlWaGIeLwzTZUQ97EC1g");
		connection.setRequestProperty("Content-Type", "text/plain");
		connection.setConnectTimeout(5000);
		connection.setReadTimeout(5000);
		}
		return connection;
	}
	
	public String forward(String message){
		
		StringBuilder resposneBuilder = new StringBuilder();
		
		try {
			
			URLConnection connection = getConn();
			
			OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
			out.write(message);
			out.close();

			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			String response = null;

			while ((response = in.readLine()) != null) {
				resposneBuilder.append(response).append(" ");
			}
			System.out.println("\nCrunchify REST Service Invoked Successfully..." + resposneBuilder);
			in.close();
		} catch (Exception e) {
			System.out.println("\nError while calling Crunchify REST Service");
			e.printStackTrace();
		}
		return resposneBuilder.toString();

	}
}
