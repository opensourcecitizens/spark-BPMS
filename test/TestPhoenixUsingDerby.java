

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neustar.iot.spark.forward.phoenix.PhoenixForwarder;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;
import io.parser.avro.phoenix.AvroToPhoenixMap;

public class TestPhoenixUsingDerby {

	private Connection conn = null;
	private ResultSet rs = null;
	private Statement smt1 = null;
	private PreparedStatement smt2 = null;
	private Statement smt3 = null;
	private Logger log = Logger.getLogger(TestPhoenixUsingDerby.class);

	@Before
	public void init() throws Exception {
		String dbURL = "org.apache.derby.jdbc.EmbeddedDriver";
		
		
		try {
			Class.forName(dbURL).newInstance();
			conn = DriverManager.getConnection("jdbc:derby:" + "droolsDB;create=true");
		} catch (Exception except) {
			except.printStackTrace();
		}

	}

	@After
	public void close() {
		try {
			if (rs != null) {
				rs.close();
			}
			if (smt1 != null) {
				smt1.close();
			}
			if (smt2 != null) {
				smt2.close();
			}
			if (smt3 != null) {
				smt3.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {

		}
	}

	
	 @Test public void testWrite() throws Exception
	 {
		 
		 if (conn != null) {
				System.out.println("connected");
		 }
		 
		 smt1 = conn.createStatement();
		 
		 try{
		 smt1.executeUpdate("CREATE TABLE TEST_TABLE (CREATED_TIME TIMESTAMP NOT NULL, id VARCHAR(255) NOT NULL, payload VARCHAR(255), PRIMARY KEY ( CREATED_TIME, id ) )");
		 }catch(Exception e){
			 System.out.println(e);
		 }
		 
		 	Schema schema = new Schema.Parser().parse(Class.class.getResourceAsStream("/CustomMessage.avsc"));
			GenericRecord mesg = new GenericData.Record(schema);		
			mesg.put("id", "device1");
			mesg.put("payload", "{'type':'internal json'}");
			
			//create avro
			byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
			
			//avro to map
			AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
			Map<String,?> map =  parser.parse(avrodata, schema);
			
			Set<String> keyset = map.keySet();
			int datasize = keyset.size();
			
			char[] qm = new char[datasize];
			for(int i = 0; i < datasize; i++){
				qm[i]='?';
			}
			smt2 = conn.prepareStatement("INSERT INTO TEST_TABLE ( "+Arrays.toString(keyset.toArray()).replace("[", "").replace("]", "")+",CREATED_TIME) "
					+ "VALUES("+Arrays.toString(qm).replace("[", "").replace("]", "")+", ? )");
			smt2.setTimestamp(datasize+1, new Timestamp(System.currentTimeMillis()));
			AvroToPhoenixMap sqlMapping = new AvroToPhoenixMap();
			
			sqlMapping.translate(smt2, map, schema);
			
			
			
			log.info(smt2);
			int res = smt2.executeUpdate();

			
			//PhoenixForwarder<String> phoenixConn = PhoenixForwarder.singleton(phoenix_zk_JDBC, new String());	
			//phoenixConn.saveToJDBC(map,schema);
			//phoenixConn.saveToJDBC(map);
			System.out.println("Sent message");
	 }
	 
	 
	@Test
	public void testClobDrool() throws Exception {

		InputStream fin = null;
		 InputStream fin2 = null;
		String sb = null;

		if (conn != null) {
			System.out.println("connected");


			 fin = TestPhoenixUsingDerby.class.getClassLoader().getResourceAsStream("drools/test/Person.drl");

			Assert.assertNotNull(fin);
			
			int a = 0;

			try {
				smt1 = conn.createStatement();
				smt3 = conn.createStatement();

				try {
					a = smt1.executeUpdate("CREATE TABLE droolstbl (name VARCHAR(26), rule CLOB(100M))");

					System.out.println("Table created");

					smt2 = conn.prepareStatement("insert into droolstbl values (?,  ? )");
					smt2.setString(1, "person_drl");
					smt2.setCharacterStream(2, new InputStreamReader(fin));
					smt2.execute();
					System.out.println("Values inserted");
				} catch (Exception e2) {
					// do nothing
					// e2.printStackTrace();
				}

				rs = smt3.executeQuery("select * from droolstbl");
				while (rs.next()) {

					System.out.println("Values in the table are: " + rs.getString(1) + ",");

					StringWriter writer = new StringWriter();

					BufferedReader read = new BufferedReader(rs.getClob(2).getCharacterStream());
					 IOUtils.copy(read, writer);
					 					 
					sb= writer.toString();

					read.close();
				}
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		 fin2 = TestPhoenixUsingDerby.class.getClassLoader().getResourceAsStream("drools/test/Person.drl");
		 StringWriter writer = new StringWriter();
	     String encoding = "UTF-8";
	     IOUtils.copy(fin2, writer, encoding);

		
		System.out.println("sb= " + sb + ",");
		System.out.println("s= " + writer.toString() + ",");
		Assert.assertEquals(writer.toString(),sb);
	}

}