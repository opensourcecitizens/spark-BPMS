package com.neustar.iot.spark.phoenix;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;

import org.apache.log4j.Logger;
import org.apache.phoenix.jdbc.PhoenixDriver;

import io.parser.avro.phoenix.AvroToPhoenixMap;

public class PhoenixForwarder<K> implements Serializable {

	Logger log = Logger.getLogger(PhoenixForwarder.class);
	private static final long serialVersionUID = 1L;

	Connection conn = null;
	PreparedStatement prepStmt = null;
	String zookeeper_quorum = null;
	private PhoenixForwarder(String _zookeeper_quorum) throws SQLException, ClassNotFoundException {
			zookeeper_quorum = _zookeeper_quorum;
	}
	
	private PhoenixForwarder(){}
	
	private static PhoenixForwarder<?> forwarderInstance = null;
	
	public static PhoenixForwarder<?> singleton(String zookeeper_quorum) throws ClassNotFoundException, SQLException{
		
		if(forwarderInstance==null){
			forwarderInstance = new PhoenixForwarder<>(zookeeper_quorum);
		}
		
		return forwarderInstance;
	} 
	
	@SuppressWarnings("unchecked")
	public static <T> PhoenixForwarder<T> singleton(String zookeeper_quorum, T keytype) throws ClassNotFoundException, SQLException{
		
		if(forwarderInstance==null){
			forwarderInstance = new PhoenixForwarder<T>(zookeeper_quorum);
		}
		
		return (PhoenixForwarder<T>) forwarderInstance;
	} 
	

	public  void saveToJDBC(String message ) throws SQLException, ClassNotFoundException {

		prepStmt = getConn().prepareStatement("UPSERT INTO test_table (ID,Message)VALUES(?,?)");
		prepStmt.setString(1, System.currentTimeMillis()+"_"+Math.random());
		prepStmt.setString(2, message);
		
		prepStmt.executeUpdate();
	}
	
	
	public <V>  void saveToJDBC(Map<String,V> map ) throws SQLException, ClassNotFoundException {

		prepStmt = getConn().prepareStatement("UPSERT INTO TEST_TABLE (CREATED_TIME,ID,MESSAGE)VALUES(?,?,?)");
		prepStmt.setTime(1, new Time(System.currentTimeMillis()));
		prepStmt.setString(2, map.get("id").toString());
		prepStmt.setString(3, map.get("payload").toString());
		log.info("SQL = "+prepStmt.toString());
		int res = prepStmt.executeUpdate();
		log.info("Execute update result = "+res);
	}
	
	/**TODO */
	public  <V> void saveToJDBC(Map<String,V> map , Schema avroSchema) throws SQLException, ClassNotFoundException {
		
		Set<String> keyset = map.keySet();
		int datasize = keyset.size();
		
		char[] qm = new char[datasize];
		for(int i = 0; i < datasize; i++){
			qm[i]='?';
		}
		
		prepStmt = getConn().prepareStatement("UPSERT INTO TEST_TABLE ( "+Arrays.toString(keyset.toArray()).replace("[", "").replace("]", "")+",CREATED_TIME) "
				+ "VALUES("+Arrays.toString(qm).replace("[", "").replace("]", "")+", ? )");
		prepStmt.setTime(datasize+1, new Time(System.currentTimeMillis()));
		AvroToPhoenixMap sqlMapping = new AvroToPhoenixMap();
		
		sqlMapping.translate(prepStmt, avroSchema, map);
		//prepStmt.setTime(3, new Time(System.currentTimeMillis()));
		log.debug("SQL = "+prepStmt.toString());
		int res = prepStmt.executeUpdate();
		log.debug("Execute update result = "+res);
	}
	
	private Connection getConn() throws SQLException, ClassNotFoundException{
		if(conn==null || conn.isClosed()){
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			DriverManager.registerDriver(new PhoenixDriver());
			conn = DriverManager.getConnection(zookeeper_quorum);
			conn.setAutoCommit(true);
		}
		return conn;
	}
	
	public synchronized void closeConn() throws SQLException {
		
			if (conn != null){
				conn.commit();
				conn.close();
			}
			if (prepStmt != null){
				prepStmt.close();
			}	
	}
	
	@Override
	public void finalize() throws Throwable{
		closeConn();
		super.finalize();
	}
}
