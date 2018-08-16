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
		// INITILIZE CACHE: cache
		// ser: main config
		CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>("sync");
		ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		ccfg.setCacheMode(CacheMode.PARTITIONED);
		ccfg.setBackups(1);
		ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
		// ser: affinity config
		RendezvousAffinityFunction affFunc = new RendezvousAffinityFunction();
		affFunc.setExcludeNeighbors(true);
		affFunc.setPartitions(1);
		ccfg.setAffinity(affFunc);
		// stale config
		NearCacheConfiguration<Integer, Integer> nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<Integer, Integer> sccfg = new CacheConfiguration<Integer, Integer>("stale_sync");
		sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		sccfg.setCacheMode(CacheMode.PARTITIONED);
		sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
		// create both caches:
		ignite.createCache(ccfg);
		ignite.createCache(sccfg,nearCfg);

	}

	public void populateAllCaches(Ignite ignite, Constants cons) {
		IgniteCache<Integer, Integer> cache = ignite.cache("sync");
		IgniteCache<Integer, Integer> stale_cache = ignite.cache("stale_sync");
		for (int i = 0; i < cons._OBJECT_NUMBER; i++) {
			System.out.print(".");
			cache.put(i, 0);
			stale_cache.put(i, 1000000);
		}

	}

	public void printAll(Ignite ignite, Constants cons) {
		int sum = 0;
		IgniteTransactions transactions = ignite.transactions();
		IgniteCache<Integer, Integer> cache = ignite.cache("sync");
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println(">> final values:");
			int currentVal = 0;
			System.out.println("key   |   value\n---------------");
			for (int i = 0; i < cons._OBJECT_NUMBER; i++) {
				currentVal = cache.get(i);
				sum += currentVal;
				System.out.println("" + i + "	|	" + currentVal + "");
			}
		}
	}

	public void destroyAll(Ignite ignite, Constants cons) {
		System.out.println(">>> destroying caches...");
		IgniteCache<Integer, Integer> cache = ignite.cache("sync");
		IgniteCache<Integer, Integer> stale_cache = ignite.cache("stale_sync");
		cache.destroy();
		stale_cache.destroy();
	}

}
