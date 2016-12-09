package com.neustar.iot.spark.cache;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.cache.LoadingCache;

public class PooledResourceCacheManager {
	
	public enum POOL_TYPE{KafkaProducerPoolCache};
	
	public final static Map<POOL_TYPE, LoadingCache> pools = new ConcurrentHashMap<POOL_TYPE, LoadingCache>();
	 
	 public static void insertCache(POOL_TYPE cacheId, LoadingCache cache) {
		 pools.put(cacheId, cache);
	    }

	 
	  public static LoadingCache<?,?> getCache(POOL_TYPE cacheId) {
	        return pools.get(cacheId);
	    }

	  
		 public static ConcurrentLinkedQueue<?>  getCachedPool(POOL_TYPE cacheId,Object elementId) {
			 ConcurrentLinkedQueue<?>  result = null;
		 
		        LoadingCache cache = getCache(cacheId);
		        if (cache != null){
		        	result = (ConcurrentLinkedQueue<?>)cache.getIfPresent(elementId);
		        }
		 
		        return result;
		    }
	  
	 public static Object pollCachedPool(POOL_TYPE cacheId,Object elementId) {
	        Object result = null;
	 
	        LoadingCache cache = getCache(cacheId);
	        if (cache != null){
	        	ConcurrentLinkedQueue<?> pool = (ConcurrentLinkedQueue<?>)cache.getIfPresent(elementId);
	            result = pool!=null?pool.poll():null;
	            if(result==null){
	            	cache.refresh(elementId);
	            }
	        }
	 
	        return result;
	    }
}
