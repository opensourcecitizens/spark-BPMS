package com.neustar.iot.spark.forward;

import com.neustar.iot.spark.forward.phoenix.PhoenixForwarder;

public class ForwarderFactory {

	enum Type{
		PHOENIX, REST_GET,REST_POST,REST_PUT, HDFS
	}
	
	static ForwarderIfc get(Type type){
		ForwarderIfc forwarder = null;
		switch(type.toString()){
			//case "PHOENIX": forwarder = PhoenixForwarder.singleton(zookeeper_quorum); break;
			case "REST": break;
		}
		
		return forwarder;
		
	}

}


