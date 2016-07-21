package com.neustar.iot.spark.forward.rest;

import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.apache.avro.Schema;

import com.neustar.iot.spark.forward.ForwarderIfc;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;


public class RestfulForwarder implements ForwarderIfc{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String uri = "http://ec2-52-41-124-186.us-west-2.compute.amazonaws.com:8080";
	private static RestfulForwarder singleton = null;
	
	private RestfulForwarder(){
	}
	
	public static ForwarderIfc singleton(String _uri) {
		
		if(singleton==null){
			singleton=new RestfulForwarder();
		}
		
		singleton.setUri(_uri);
		return singleton;
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
	public String forward(Map<String, ?> map, Schema schema) throws Throwable {
		
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
		builder.header("API-KEY", "1");

		ClientResponse cliResponse = builder.get(ClientResponse.class);
		
		return cliResponse.getEntity(String.class);
	}
	
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}

}
