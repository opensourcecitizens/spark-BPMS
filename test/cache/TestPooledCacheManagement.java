package cache;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.neustar.iot.spark.cache.PooledResourceCacheManager;

public class TestPooledCacheManagement {
	Properties kafkaProps = null;
	
	@Before
	public void init(){
		
		kafkaProps = new Properties();
		kafkaProps.put("size", 10);
		kafkaProps.put("name", "kaniu_pool");

	}
	
	
	@Test
	public void runTest() throws ExecutionException, InterruptedException{
		
		for(int i =0 ; i<20; i++){
			System.out.println("consuming "+i);
			testMethod(kafkaProps);
			Thread.sleep(10);
			
		}
		
	}
	
	@After
	public void cleanup(){
		PooledResourceCacheManager.getCache(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache) .invalidateAll();
	}
	
	
	protected MyKafkaProducer testMethod(Properties kafkaProps ) throws ExecutionException{
		
		LoadingCache<Properties,ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>>> cache = null;
		
		if( PooledResourceCacheManager.getCache(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache) !=null){
			if(!PooledResourceCacheManager.getCachedPool(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache, kafkaProps).isEmpty()) {
				return (MyKafkaProducer) PooledResourceCacheManager.pollCachedPool(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache, kafkaProps);
			}
		}
		
		CacheLoader<Properties,ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>>> loader = new CacheLoader<Properties,ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>>>(){
			@Override
			public ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>> load(Properties key) throws Exception {
				ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>> queue = new ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>>();
				
				for(int i = 0 ; i < (int)key.get("size");i++){
				MyKafkaProducer<String, byte[]> producer = new MyKafkaProducer<String, byte[]>(key);
				
				queue.add(producer);
				System.out.println("creating "+i);
				}
				
				return queue;
			}
		};
		
		RemovalListener<Properties,ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>>> removalListener = new RemovalListener<Properties,ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>>>() {
			  public void onRemoval(RemovalNotification<Properties,ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>>> removal) {
				  ConcurrentLinkedQueue<MyKafkaProducer<String, byte[]>> queue = removal.getValue();
				  while(!queue.isEmpty()){
				  MyKafkaProducer<String, byte[]> conn = queue.poll();
				  conn.close(); 
				  }
			  }

			};
			
			//removalListener(removalListener)

		cache = CacheBuilder.newBuilder().maximumSize(10).refreshAfterWrite((long)100, TimeUnit.MILLISECONDS).removalListener(removalListener).build(loader);

		PooledResourceCacheManager.insertCache(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache, cache);
		
		return (MyKafkaProducer) PooledResourceCacheManager.pollCachedPool(PooledResourceCacheManager.POOL_TYPE.KafkaProducerPoolCache, kafkaProps);

	}
	
	
	public class MyKafkaProducer<K,V>{
		public MyKafkaProducer(Properties props){
			
		}
		
		public void close(){
			System.out.println("closing kafka consumer ");
		}
	}
}
