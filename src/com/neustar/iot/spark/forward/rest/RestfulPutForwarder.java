package com.neustar.iot.spark.forward.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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

import io.parser.avro.AvroUtils;


public class RestfulPutForwarder implements ForwarderIfc{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String uri = "http://ec2-52-41-124-186.us-west-2.compute.amazonaws.com:8080";
	private static RestfulPutForwarder singleton = null;
	
	private RestfulPutForwarder(){
	}
	
	private RestfulPutForwarder(String _uri){
		uri = _uri;
	}
	
	public static ForwarderIfc singleton(String _uri) {
		
		if(singleton==null){
			singleton=new RestfulPutForwarder();
		}
		
		singleton.setUri(_uri);
		return singleton;
	}
	
	
	public static ForwarderIfc instance(String _uri) {
		return new RestfulPutForwarder(_uri);
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
		String path = (String) map.get("payload");
		
		webResource = getWebResource().path(path);
		Builder builder = webResource.accept(MediaType.APPLICATION_JSON);
		builder.type(MediaType.APPLICATION_JSON);

		ClientResponse cliResponse = builder.put(ClientResponse.class);
		
		return cliResponse.getEntity(String.class);
	}
	
	@Override
	public String forward(Map<String, ?> map, Schema schema, Map<String, ?> attrMap) throws Throwable {

		String payloadJsonStr =  (String) (map.get("payload") instanceof Map? ((Map)map.get("payload")).get("string"):map.get("payload"));

		Map<String,?> headerMap = new HashMap<String,Object>();
		String path = "/";
		
		ObjectMapper mapper = new ObjectMapper();
		if(attrMap!=null){
		path = attrMap.get("path")!=null?attrMap.get("path").toString():"";	
		System.out.println(path);
				
		
		if(attrMap.get("header")!=null){
			headerMap = mapper.readValue( attrMap.get("header").toString(),new TypeReference<Map<String, ?>>(){});
		}
		
		}
		
		Set<String> headerkeys = headerMap.keySet();
		
		webResource = getWebResource().path(path);
		Builder builder = webResource.accept(MediaType.APPLICATION_JSON);
		for(String headerkey : headerkeys){
			builder.header(headerkey, headerMap.get(headerkey));
			System.out.println("HEADER "+headerkey+"  :  "+ headerMap.get(headerkey));
		}
		
		//System.out.println(path);
		//System.out.println(payloadJsonStr);
		//return "";
		
		ClientResponse cliResponse = builder.put(ClientResponse.class, payloadJsonStr);
			
		return cliResponse.getEntity(String.class);
		
	}
	
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}

}
