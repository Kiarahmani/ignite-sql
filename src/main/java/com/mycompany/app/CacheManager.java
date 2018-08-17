package com.mycompany.app;

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

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

	// 2
	public void createAllCaches(Ignite ignite) {
		// INITILIZE CACHE: district
		// ser: main config
		CacheConfiguration<DoubleKey, District> district_ccfg = new CacheConfiguration<DoubleKey, District>(
				"district_ser");
		district_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		district_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction affFunc = new RendezvousAffinityFunction();
		affFunc.setExcludeNeighbors(true);
		affFunc.setPartitions(1);
		district_ccfg.setAffinity(affFunc);
		// stale config
		NearCacheConfiguration<DoubleKey, District> district_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<DoubleKey, District> district_sccfg = new CacheConfiguration<DoubleKey, District>(
				"district_stale");
		district_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		district_sccfg.setCacheMode(CacheMode.REPLICATED);
		district_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(district_ccfg);
		ignite.createCache(district_sccfg, district_nearCfg);
		// ----------------------------------------------------------------------------------------------------
		// INITILIZE CACHE: warehouse
		// ser: main config
		CacheConfiguration<Integer, Warehouse> warehouse_ccfg = new CacheConfiguration<Integer, Warehouse>(
				"warehouse_ser");
		warehouse_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		warehouse_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction warehouse_affFunc = new RendezvousAffinityFunction();
		warehouse_affFunc.setExcludeNeighbors(true);
		warehouse_affFunc.setPartitions(1);
		warehouse_ccfg.setAffinity(warehouse_affFunc);
		// stale config
		NearCacheConfiguration<Integer, Warehouse> warehouse_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<Integer, Warehouse> warehouse_sccfg = new CacheConfiguration<Integer, Warehouse>(
				"warehouse_stale");
		warehouse_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		warehouse_sccfg.setCacheMode(CacheMode.REPLICATED);
		warehouse_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(warehouse_ccfg);
		ignite.createCache(warehouse_sccfg, warehouse_nearCfg);
		// ----------------------------------------------------------------------------------------------------
		// ----------------------------------------------------------------------------------------------------
		// INITILIZE CACHE: customer
		// ser: main config
		CacheConfiguration<TrippleKey, Customer> customer_ccfg = new CacheConfiguration<TrippleKey, Customer>(
				"customer_ser");
		customer_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		customer_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction customer_affFunc = new RendezvousAffinityFunction();
		customer_affFunc.setExcludeNeighbors(true);
		customer_affFunc.setPartitions(1);
		customer_ccfg.setAffinity(customer_affFunc);
		// stale config
		NearCacheConfiguration<TrippleKey, Customer> customer_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<TrippleKey, Customer> customer_sccfg = new CacheConfiguration<TrippleKey, Customer>(
				"customer_stale");
		customer_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		customer_sccfg.setCacheMode(CacheMode.REPLICATED);
		customer_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(customer_ccfg);
		ignite.createCache(customer_sccfg, warehouse_nearCfg);
		// ----------------------------------------------------------------------------------------------------

	}

	public void populateAllCaches(Ignite ignite, Constants cons) {
		IgniteCache<DoubleKey, District> district_cache = ignite.cache("district_ser");
		IgniteCache<DoubleKey, District> district_scache = ignite.cache("district_stale");
		IgniteCache<Integer, Warehouse> warehouse_cache = ignite.cache("warehouse_ser");
		IgniteCache<Integer, Warehouse> warehouse_scache = ignite.cache("warehouse_stale");
		IgniteCache<TrippleKey, Customer> customer_cache = ignite.cache("customer_ser");
		IgniteCache<TrippleKey, Customer> customer_scache = ignite.cache("customer_stale");
		System.out.print("\n\nINITIALIZATION\n===========================================\n");
		// district
		for (DoubleKey key : cons.all_keys_district) {
			System.out.println("init(district)" + key.toString());
			String name = "D" + UUID.randomUUID().toString().substring(0, 5);
			String address = "D" + UUID.randomUUID().toString().substring(0, 5);
			district_cache.put(key, new District(name, address, 0, 0, true));
			district_scache.put(key, new District(name, address, 0, 0, true));
		}
		// warehouse
		for (int key : cons.all_keys_warehouse) {
			System.out.println("init(warehouse)" + key);
			String name = "W" + UUID.randomUUID().toString().substring(0, 5);
			String address = "W" + UUID.randomUUID().toString().substring(0, 5);
			warehouse_cache.put(key, new Warehouse(name, address, 0, 0, true));
			warehouse_scache.put(key, new Warehouse(name, address, 0, 0, true));
		}
		// customer
		for (TrippleKey key : cons.all_keys_customer) {
			System.out.println("init(customer)" + key.toString());
			String name = "C" + UUID.randomUUID().toString().substring(0, 5);
			String address = "C" + UUID.randomUUID().toString().substring(0, 5);
			int balance = ThreadLocalRandom.current().nextInt(100, 100000);
			int discount = ThreadLocalRandom.current().nextInt(0, 15);
			int credit = ThreadLocalRandom.current().nextInt(0, 2);
			customer_cache.put(key, new Customer(name, address, balance, discount, credit, 0, 0, 0, true));
			customer_scache.put(key, new Customer(name, address, balance, discount, credit, 0, 0, 0, true));

		}

	}

	public void printAll(Ignite ignite, Constants cons) {
		IgniteTransactions transactions = ignite.transactions();
		IgniteCache<DoubleKey, District> district_cache = ignite.cache("district_ser");
		IgniteCache<Integer, Warehouse> warehouse_cache = ignite.cache("warehouse_ser");
		IgniteCache<TrippleKey, Warehouse> customer_cache = ignite.cache("customer_ser");
		// distrcit
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<districts>>");
			System.out.println(
					"----------------------------------\nkey     |   value\n----------------------------------");
			for (DoubleKey key : cons.all_keys_district) {
				System.out.println("" + key.toString() + "	| " + district_cache.get(key).toString() + "");
			}
		}
		// warehouse
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<warehouses>>");
			System.out.println(
					"----------------------------------\nkey     |   value\n----------------------------------");
			for (int key : cons.all_keys_warehouse) {
				System.out.println("$(" + key + ")	| " + warehouse_cache.get(key).toString() + "");
			}
		}
		// customer
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<customer>>");
			System.out.println(
					"----------------------------------\nkey     |   value\n----------------------------------");
			for (TrippleKey key : cons.all_keys_customer) {
				System.out.println(key.toString() + "	| " + customer_cache.get(key).toString() + "");
			}
		}

	}

	public void destroyAll(Ignite ignite, Constants cons) {
		System.out.println("\n>\n>\n>>> destroying caches...");
		IgniteCache<DoubleKey, District> district_cache = ignite.cache("district_ser");
		IgniteCache<DoubleKey, District> district_scache = ignite.cache("district_stale");
		IgniteCache<Integer, Warehouse> warehouse_cache = ignite.cache("warehouse_ser");
		IgniteCache<Integer, Warehouse> warehouse_scache = ignite.cache("warehouse_stale");
		IgniteCache<TrippleKey, Customer> customer_cache = ignite.cache("customer_ser");
		IgniteCache<TrippleKey, Customer> customer_scache = ignite.cache("customer_stale");
		district_cache.destroy();
		district_scache.destroy();
		warehouse_cache.destroy();
		warehouse_scache.destroy();
		customer_cache.destroy();
		customer_scache.destroy();
	}

}
