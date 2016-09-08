package com.neustar.iot.spark.forward.hdfs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.neustar.iot.spark.forward.ForwarderIfc;
import com.sun.jersey.api.client.WebResource.Builder;

public class HDFSForwarder implements ForwarderIfc {

	Logger log = Logger.getLogger(HDFSForwarder.class);
	private static final long serialVersionUID = 1L;

	String hdfsuri = null;
	private HDFSForwarder(String uri) throws SQLException, ClassNotFoundException {
			hdfsuri = uri;// = _zookeeper_quorum;
	}
	
	private HDFSForwarder(){}
	
	private static HDFSForwarder forwarderInstance = null;
	
	public static HDFSForwarder singleton(String zookeeper_quorum) throws ClassNotFoundException, SQLException{
		
		if(forwarderInstance==null){
			forwarderInstance = new HDFSForwarder(zookeeper_quorum);
		}
		
		return forwarderInstance;
	} 

	
	
	@Override
	public void finalize() throws Throwable{
		
		super.finalize();
	}

	

	@Override
	public synchronized String forward(Map<String, ?> map, Schema schema) throws Throwable {

		String ret = "SUCCESS";
		try{
			
			ObjectMapper mapper = new ObjectMapper();
			String json = mapper.writeValueAsString(map);
			
			appendToHDFS(hdfsuri,json);
			
			
		}catch(Exception e){
			log.error(e,e);
			ret = "ERROR due to"+e.getMessage();
		}
		
		log.info("appending to "+hdfsuri+" returned "+ret);
		
		return  ret;
	}
	
	protected  Configuration createHDFSConfiguration() {

		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		return hadoopConfig;
	}
	
	protected  void appendToHDFS(String pathStr, String data) throws IOException {

		Path path = new Path(pathStr);
		Configuration conf = createHDFSConfiguration();
		FileSystem fs = path.getFileSystem(conf);

		try {

			if (!fs.exists(path)) {
				fs.create(path);
				fs.setReplication(path,  (short)1);
				fs.close();
			}
			 fs = path.getFileSystem(conf);
			 fs.setReplication(path,  (short)1);
			FSDataOutputStream out = fs.append(path);
			out.writeUTF(data);
		} finally {
			fs.close();
		}

	}

	@Override
	public String forward(Map<String, ?> map, Schema schema, Map<String, ?> attr) throws Throwable {
		return forward(map,schema);
	}
}
