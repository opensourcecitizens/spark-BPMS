package com.neustar.iot.spark.forward.phoenix;

import java.io.Serializable;

public class MessageRow implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String id;
	private String message;
	
	public String getMessage() {
		return message;
	}
	
	public void setMessage(String message) {
		this.message = message;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}

}
