package com.neustar.iot.spark.phoenix;

public class Parser {
	
	 public MessageRow parse(String inStr)
	    {
		 MessageRow row = new MessageRow();
		 row.setMessage(inStr);
		 row.setId(System.currentTimeMillis()+"_"+Math.random());

	      return row;
	    }
}
