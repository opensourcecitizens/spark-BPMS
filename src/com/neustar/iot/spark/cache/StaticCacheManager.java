package com.neustar.iot.spark.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.cache.LoadingCache;

public class StaticCacheManager {
	
	 public enum CACHE_TYPE {RulesCache, RestPostCache, RestGetCache, WebSchemaCache, PropertiesCache, HdfsSchemaCache, PhoenixForwarderCache, KafkaProducerCache};
	 
	 public final static Map<CACHE_TYPE, LoadingCache> caches = new ConcurrentHashMap<CACHE_TYPE, LoadingCache>();
	 
	 public static void insertCache(CACHE_TYPE cacheId, LoadingCache cache) {
	        caches.put(cacheId, cache);
	    }

	 
	  public static LoadingCache<?,?> getCache(CACHE_TYPE cacheId) {
	        return caches.get(cacheId);
	    }
	  
	 public static Object getCachedObject(CACHE_TYPE cacheId, Comparable elementId) {
	        Object result = null;
	 
	        LoadingCache cache = getCache(cacheId);
	        if (cache != null){
	            result = cache.getIfPresent(elementId);
	        }
	 
	        return result;
	    }
}
