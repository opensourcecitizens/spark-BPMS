package com.neustar.iot.spark.forward.rest;

import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.apache.avro.Schema;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.neustar.iot.spark.forward.ForwarderIfc;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;


public class RestfulPostForwarder implements ForwarderIfc{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String uri = "http://ec2-52-41-124-186.us-west-2.compute.amazonaws.com:8080";
	private static RestfulPostForwarder singleton = null;
	
	private RestfulPostForwarder(){
	}
	
	private RestfulPostForwarder(String _uri){
		uri= _uri;
	}
	
	public static ForwarderIfc singleton(String _uri) {
		
		if(singleton==null){
			singleton=new RestfulPostForwarder();
		}
		
		singleton.setUri(_uri);
		return singleton;
	}
	
	public static ForwarderIfc instance(String _uri) {
		return new RestfulPostForwarder(_uri);
	}

	
	private WebResource webResource = null;

	//String 
	public WebResource getWebResource(){
		if(webResource==null){
		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		 webResource = client.resource(UriBuilder.fromUri(uri).build());
		}
		return webResource;
	}
	@Override
	public synchronized String forward(Map<String, ?> map, Schema schema) throws Throwable {
		
		/*
				
				MultivaluedMap formData = new MultivaluedMapImpl();
		 		formData.add('name1', 'val1');
		  		formData.add('name2', 'val2');
		  		
		  		builder.put(ClientResponse.class,formData);
		 */
		
		String path = (String) map.get("payload");
		
		webResource = getWebResource().path(path);
		Builder builder = webResource.accept(MediaType.APPLICATION_JSON);
		builder.type(MediaType.APPLICATION_JSON);
		//builder.header("API-KEY", "1");

		ClientResponse cliResponse = builder.post(ClientResponse.class);
		
		return cliResponse.getEntity(String.class);
	}
	
	@Override
	public String forward(Map<String, ?> map, Schema schema, Map<String, ?> attrMap) throws Throwable {

		String payloadJsonStr = (String) map.get("payload");
		//System.out.println(payloadJsonStr);
		
		ObjectMapper mapper = new ObjectMapper();
		Map<String,?> payload = 	mapper.readValue(payloadJsonStr, new TypeReference<Map<String, ?>>(){});	
		
		Map<String, ?> attr = mapper.readValue(mapper.writeValueAsString(attrMap), new TypeReference<Map<String, ?>>(){});
		//System.out.println(mapper.writeValueAsString(attrMap));
		String path = attr.get("path").toString();	
		//System.out.println(path);
		
	
		Map<String,?> headerMap = mapper.readValue( attr.get("header").toString(),new TypeReference<Map<String, ?>>(){});
		Set<String> headerkeys = headerMap.keySet();
		//String apikey = headerMap.get("API-KEY").toString();
		//System.out.println(apikey);
		
		//String contentType = headerMap.get("Content-Type")!=null?headerMap.get("Content-Type").toString():MediaType.APPLICATION_JSON;
		//System.out.println(contentType);

		
		webResource = getWebResource().path(path);
		Builder builder = webResource.accept(MediaType.APPLICATION_JSON);
		
		for(String headerkey : headerkeys){
			builder.header(headerkey, headerMap.get(headerkey));
		}
		
		ClientResponse cliResponse = builder.post(ClientResponse.class, mapper.writeValueAsString(payload));
		
		return cliResponse.getEntity(String.class);
	}
	
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}

}
