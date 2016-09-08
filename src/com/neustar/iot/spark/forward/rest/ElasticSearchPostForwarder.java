package com.neustar.iot.spark.forward.rest;

import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.apache.avro.Schema;
import org.codehaus.jackson.map.ObjectMapper;

import com.neustar.iot.spark.forward.ForwarderIfc;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;


public class ElasticSearchPostForwarder implements ForwarderIfc{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String uri = "https://search-iotaselasticsearch-qtpuykpxgabuzfidzncsfyp7k4.us-west-2.es.amazonaws.com/ioteventindex";
	private static ElasticSearchPostForwarder singleton = null;
	
	private ElasticSearchPostForwarder(){
	}
	
	private ElasticSearchPostForwarder(String _uri){
		uri= _uri;
	}
	
	public static ForwarderIfc singleton(String _uri) {
		
		if(singleton==null){
			singleton=new ElasticSearchPostForwarder();
		}
		
		singleton.setUri(_uri);
		return singleton;
	}
	
	public static ForwarderIfc instance(String _uri) {
		return new ElasticSearchPostForwarder(_uri);
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
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(map);
		
		webResource = getWebResource();//.path(path);
		Builder builder = webResource.accept(MediaType.APPLICATION_JSON);
		builder.type(MediaType.APPLICATION_JSON );
		//builder.header("API-KEY", "1");

		ClientResponse cliResponse = builder.post(ClientResponse.class, json);
		
		return cliResponse.getEntity(String.class);
	}
	
	@Override
	public String forward(Map<String, ?> map, Schema schema, Map<String, ?> attr) throws Throwable {

		return forward(map,schema);
	}
	
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}

}
