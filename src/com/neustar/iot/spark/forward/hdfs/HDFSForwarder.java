package com.neustar.iot.spark.forward.hdfs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.neustar.iot.spark.forward.ForwarderIfc;

public class HDFSForwarder implements ForwarderIfc {

	Logger log = Logger.getLogger(HDFSForwarder.class);
	private static final long serialVersionUID = 1L;

	Connection conn = null;
	PreparedStatement prepStmt = null;
	String zookeeper_quorum = null;
	private HDFSForwarder(String _zookeeper_quorum) throws SQLException, ClassNotFoundException {
			zookeeper_quorum = _zookeeper_quorum;
	}
	
	private HDFSForwarder(){}
	
	private static HDFSForwarder forwarderInstance = null;
	
	public static HDFSForwarder singleton(String zookeeper_quorum) throws ClassNotFoundException, SQLException{
		
		if(forwarderInstance==null){
			forwarderInstance = new HDFSForwarder(zookeeper_quorum);
		}
		
		return forwarderInstance;
	} 
	


	
	private Connection getConn() throws SQLException, ClassNotFoundException{
		
		return conn;
	}
	
	public synchronized void closeConn() throws SQLException {
		
		
	}
	
	@Override
	public void finalize() throws Throwable{
		closeConn();
		super.finalize();
	}

	

	@Override
	public String forward(Map<String, ?> map, Schema schema) throws Throwable {
		Set<String> keyset = map.keySet();
		int datasize = keyset.size();
		
		char[] qm = new char[datasize];
		for(int i = 0; i < datasize; i++){
			qm[i]='?';
		}
		
		
		
		return  null;
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
}
