package com.neustar.iot.spark.forward.phoenix;

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
import org.codehaus.jackson.map.ObjectMapper;

import com.neustar.iot.spark.forward.ForwarderIfc;

import io.parser.avro.phoenix.AvroToPhoenixMap;

public class PhoenixForwarder implements ForwarderIfc {

	Logger log = Logger.getLogger(PhoenixForwarder.class);
	private static final long serialVersionUID = 1L;

	Connection conn = null;
	PreparedStatement prepStmt = null;
	private String jdbcUrl = null;
	private String tablename = "TEST_TABLE";

	private PhoenixForwarder(String _jdbcUrl, String _tablename) throws SQLException, ClassNotFoundException {
			jdbcUrl = _jdbcUrl;
			tablename = _tablename;
	}
	
	private PhoenixForwarder(){}
	
	private static PhoenixForwarder singleton = null;
	
	public static PhoenixForwarder singleton(String _jdbcUrl) throws ClassNotFoundException, SQLException{
		
		if(singleton==null){
			singleton = new PhoenixForwarder();
			singleton.setJdbcUrl(_jdbcUrl);
		}
		
		return singleton;
	} 
	
	public static PhoenixForwarder instance(String _jdbcUrl, String _tablename) throws ClassNotFoundException, SQLException{
		return new PhoenixForwarder(_jdbcUrl, _tablename);
	}
	
	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}


	private Connection getConn() throws SQLException, ClassNotFoundException{
		if(conn==null || conn.isClosed()){
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			DriverManager.registerDriver(new PhoenixDriver());
			conn = DriverManager.getConnection(jdbcUrl);
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

	

	@Override
	public synchronized String forward(Map<String, ?> map, Schema schema) throws Throwable {
		ObjectMapper jsonmapper = new ObjectMapper();
		String rawjson = jsonmapper.writeValueAsString(map);
		Set<String> keyset = map.keySet();
		int datasize = keyset.size();
		
		char[] qm = new char[datasize];
		for(int i = 0; i < datasize; i++){
			qm[i]='?';
		}
		
		prepStmt = getConn().prepareStatement("UPSERT INTO "+tablename+" ( "+Arrays.toString(keyset.toArray()).replace("[", "").replace("]", "")+",CREATED_TIME, RAW_JSON) "
				+ "VALUES("+Arrays.toString(qm).replace("[", "").replace("]", "")+", ? ,?)");
		
		AvroToPhoenixMap sqlMapping = new AvroToPhoenixMap();
		
		sqlMapping.translate(prepStmt, map, schema);
		
		prepStmt.setTime(datasize+1, new Time(System.currentTimeMillis()));
		prepStmt.setString(datasize+2, rawjson);
		log.debug("SQL = "+prepStmt.toString());
		int res = prepStmt.executeUpdate();
		log.debug("Execute update result = "+res);
		
		
		return  res+"";
	}

	@Override
	public String forward(Map<String, ?> map, Schema schema, Map<String, ?> attr) throws Throwable {

		return forward(map,schema);
	}
}
