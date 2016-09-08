package com.neustar.iot.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import io.parser.avro.AvroParser;

public abstract class AbstractStreamProcess implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6060091345310773499L;


	public synchronized Configuration createHDFSConfiguration() {

		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		return hadoopConfig;
	}
	
	public  synchronized void appendToHDFS(String pathStr, String data) throws IOException {

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
	
	
	public synchronized Schema readSchemaFromHDFS(Schema.Parser parser,String uri) throws IOException{

		Configuration conf = createHDFSConfiguration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		 
		Schema ret = null;
		try {
			in = fs.open(new Path(uri));
			ret = parser.parse(in);
		} finally {
			IOUtils.closeStream(in);
		}
		return ret;
	}
	
	
	/**
	 * Added Guava caching
	 * */
	public synchronized Schema retrieveLatestAvroSchema(String avro_schema_hdfs_location) throws IOException, ExecutionException{
		CacheLoader<String,Schema> loader = new CacheLoader<String,Schema>(){
			@Override
			public Schema load(String key) throws Exception {
				Schema.Parser parser = new Schema.Parser();
				return readSchemaFromHDFS(parser, key);
			}
		};
		
		LoadingCache<String, Schema> cache = CacheBuilder.newBuilder().refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);
			
		return cache.get(avro_schema_hdfs_location);		
	}
	
	
	public synchronized Properties retrieveLocalPackageProperties(final String propfile) throws IOException, ExecutionException{
		CacheLoader<String,Properties> loader = new CacheLoader<String,Properties>(){
			@Override
			public Properties load(String key) throws Exception {
				InputStream props = CacheLoader.class.getClassLoader().getResourceAsStream(propfile);
				Properties properties = new Properties();
				properties.load(props);
				
				return properties;
			}
		};
		
		LoadingCache<String, Properties> cache = CacheBuilder.newBuilder().refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);
			
		return cache.get(propfile);		
	}
	
	
	public Map<String,?> parseAvroData(byte[] avrodata, String avro_schema_hdfs_location) throws Exception{
		Schema schema = retrieveLatestAvroSchema(avro_schema_hdfs_location);
		AvroParser<Map<String,?>> avroParser = new AvroParser<Map<String,?>>(schema);
		return avroParser.parse(avrodata, new HashMap<String,Object>());		
	}
	
	public String parseAvroData(byte[] avrodata, String avro_schema_hdfs_location, Class<String> type) throws Exception{
		Schema schema = retrieveLatestAvroSchema(avro_schema_hdfs_location);
		AvroParser<String> avroParser = new AvroParser<String>(schema);
		return avroParser.parse(avrodata, new String());		
	}
	
	

}
