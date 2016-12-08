package com.neustar.iot.spark.rules;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.kie.api.KieServices;
import org.kie.api.io.Resource;
import org.kie.api.io.ResourceType;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.neustar.iot.spark.cache.StaticCacheManager;

import io.rules.drools.StatelessRuleRunner;

public class RulesProxy implements  java.io.Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2894662285905184948L;
	
	private static final Logger log = Logger.getLogger(RulesProxy.class);
	
	private StatelessRuleRunner runner = new StatelessRuleRunner();
	
	public static RulesProxy instance(){
		return new RulesProxy();
	}
	
	private static RulesProxy singleton = null;
	
	public static RulesProxy singleton(){
		if(singleton==null){
			singleton = new RulesProxy();
		}
		return singleton;
	}
	
	public void executeRules(Map<String,?> map) throws IOException{
		
		String customerId = (String) map.get("sourceid");
		String rulesAsString;
		
		try {
			rulesAsString = queryForCachedRules(customerId);
		} catch (ExecutionException e) {
			log.warn(e,e);
			rulesAsString = queryForRules(customerId);
		}
			
		Resource resource = KieServices.Factory.get().getResources().newByteArrayResource(rulesAsString.getBytes(),"UTF-8");
		resource.setTargetPath("src/main/resources/"+customerId+"customer_drl");
		resource.setResourceType(ResourceType.DRL );
		
		Resource [] rules = {resource};
		Map<?,?> [] facts = { map };
		Map<?,?>[] ret = runner.runRules(rules, facts);	
		
		String s = Arrays.toString(ret);
		
		log.info(s);
	}
	
	private String queryForCachedRules(final String customerId) throws IOException, ExecutionException {
		
		LoadingCache<String, String> cache = null;
		
		if((cache = (LoadingCache<String, String>) StaticCacheManager.getCache(StaticCacheManager.CACHE_TYPE.RulesCache))!=null){
			return cache.get(customerId);
		}
			
		CacheLoader<String,String> loader = new CacheLoader<String,String>(){
			@Override
			public String load(String key) throws Exception {
				return queryForRules(key);
			}
		};
		
		cache = CacheBuilder.newBuilder().
				refreshAfterWrite((long)1, TimeUnit.HOURS).build(loader);
		
		StaticCacheManager.insertCache(StaticCacheManager.CACHE_TYPE.RulesCache, cache);	
		
		return cache.get(customerId);
	}
	
	private String queryForRules(String customerId) throws IOException {
		String ret = null;
		System.out.println("uniqueid = "+customerId);
		InputStream rulesStream = new RulesForwardWorker().retrieveRulesFromHDFS(customerId);
		StringWriter writer = new StringWriter();
		try{
			IOUtils.copy(rulesStream, writer);
			ret = writer.getBuffer().toString();
		}finally{
			writer.close();
			rulesStream.close();
		}
		return ret;
	}

}
