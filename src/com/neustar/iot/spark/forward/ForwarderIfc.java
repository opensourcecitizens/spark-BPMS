package com.neustar.iot.spark.forward;

import java.io.Serializable;
import java.util.Map;

import org.apache.avro.Schema;


public interface ForwarderIfc extends Serializable{
	
	public String forward( Map<String,?>map, Schema schema) throws Throwable; 
	public String forward( Map<String,?>map, Schema schema, Map<String,?> attr) throws Throwable; 
	
	//public String forward( Object data, Schema schema) throws Throwable; 
	//public String forward( Object data, Schema schema, Map<String,?> attr) throws Throwable; 
	
}
