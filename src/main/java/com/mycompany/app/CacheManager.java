package com.mycompany.app;

import java.util.Set;
import java.util.TreeSet;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;

public class CacheManager {

	public CacheManager(Ignite ignite) {
		System.out.println(">>>> CacheManager: currently available caches: " + ignite.cacheNames());
	}

	public void createAllCaches(Ignite ignite) {
		// INITILIZE CACHE: district
		// ser: main config
		CacheConfiguration<DoubleKey, Integer> district_ccfg = new CacheConfiguration<DoubleKey, Integer>(
				"district_ser");
		district_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		district_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction affFunc = new RendezvousAffinityFunction();
		affFunc.setExcludeNeighbors(true);
		affFunc.setPartitions(1);
		district_ccfg.setAffinity(affFunc);
		// stale config
		NearCacheConfiguration<DoubleKey, Integer> district_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<DoubleKey, Integer> district_sccfg = new CacheConfiguration<DoubleKey, Integer>(
				"district_stale");
		district_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		district_sccfg.setCacheMode(CacheMode.REPLICATED);
		district_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(district_ccfg);
		ignite.createCache(district_sccfg, district_nearCfg);
		// ----------------------------------------------------------------------------------------------------
	}

	public void populateAllCaches(Ignite ignite, Constants cons) {
		IgniteCache<DoubleKey, Integer> district_cache = ignite.cache("district_ser");
		IgniteCache<DoubleKey, Integer> district_scache = ignite.cache("district_stale");
		for (DoubleKey key : cons.all_keys_district) {
			System.out.println("init(district)#" + key.toString());
			district_cache.put(key, 0);
			district_scache.put(key, 1000000);
		}

	}

	public void printAll(Ignite ignite, Constants cons) {
		IgniteTransactions transactions = ignite.transactions();
		IgniteCache<DoubleKey, Integer> district_cache = ignite.cache("district_ser");
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n>>districts:");
			System.out.println("key     |   value\n-----------------");
			for (DoubleKey key : cons.all_keys_district) {
				System.out.println("" + key.toString() + "	| " + district_cache.get(key) + "");
			}
		}
	}

	public void destroyAll(Ignite ignite, Constants cons) {
		System.out.println(">>> destroying caches...");
		IgniteCache<DoubleKey, Integer> district_cache = ignite.cache("district_ser");
		IgniteCache<DoubleKey, Integer> district_scache = ignite.cache("district_stale");
		district_cache.destroy();
		district_scache.destroy();
		System.out.println("######destroyAll###" + cons.all_keys_district.size());
	}

}
