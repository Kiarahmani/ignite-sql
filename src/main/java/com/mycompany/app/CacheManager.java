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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;

public class CacheManager {

	public CacheManager(Ignite ignite) {
		System.out.println(">>>> CacheManager: currently available caches: " + ignite.cacheNames());
	}

	public void createCoordinationCaches(Ignite ignite, int followerCount) {
		CacheConfiguration<String, Integer> coordination_ccfg = new CacheConfiguration<String, Integer>("coordination");
		coordination_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		coordination_ccfg.setCacheMode(CacheMode.REPLICATED);
		IgniteCache<String, Integer> coordination_cache = ignite.createCache(coordination_ccfg);
		coordination_cache.put("ready", 0);
		coordination_cache.put("initialized", 0);
	}

	public void dispatchFollowers(Ignite ignite) {
		IgniteCache<String, Integer> coordination_cache = ignite.cache("coordination");
		coordination_cache.put("initialized", 1);

	}

	public void waitForFollowers(Ignite ignite, int followerCount) {
		IgniteCache<String, Integer> coordination_cache = ignite.cache("coordination");
		do {
			System.out.println("+++manager waiting for followers to finish");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} while (coordination_cache.get("finished") < followerCount);
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
		ignite.createCache(customer_sccfg, customer_nearCfg);
		// ----------------------------------------------------------------------------------------------------
		// ----------------------------------------------------------------------------------------------------
		// INITILIZE CACHE: orders
		// ser: main config
		CacheConfiguration<QuadKey, Order> order_ccfg = new CacheConfiguration<QuadKey, Order>("order_ser");
		order_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		order_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction order_affFunc = new RendezvousAffinityFunction();
		order_affFunc.setExcludeNeighbors(true);
		order_affFunc.setPartitions(1);
		order_ccfg.setAffinity(order_affFunc);
		// stale config
		NearCacheConfiguration<QuadKey, Order> order_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<QuadKey, Order> order_sccfg = new CacheConfiguration<QuadKey, Order>("order_stale");
		order_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		order_sccfg.setCacheMode(CacheMode.REPLICATED);
		order_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(order_ccfg);
		ignite.createCache(order_sccfg, order_nearCfg);
		// ----------------------------------------------------------------------------------------------------
		// ----------------------------------------------------------------------------------------------------
		// INITILIZE CACHE: ordersLine
		// ser: main config
		CacheConfiguration<TrippleKey, OrderLine> orderLine_ccfg = new CacheConfiguration<TrippleKey, OrderLine>(
				"orderLine_ser");
		orderLine_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		orderLine_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction orderLine_affFunc = new RendezvousAffinityFunction();
		orderLine_affFunc.setExcludeNeighbors(true);
		orderLine_affFunc.setPartitions(1);
		orderLine_ccfg.setAffinity(orderLine_affFunc);
		// stale config
		NearCacheConfiguration<TrippleKey, OrderLine> orderLine_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<TrippleKey, OrderLine> orderLine_sccfg = new CacheConfiguration<TrippleKey, OrderLine>(
				"orderLine_stale");
		orderLine_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		orderLine_sccfg.setCacheMode(CacheMode.REPLICATED);
		orderLine_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(orderLine_ccfg);
		ignite.createCache(orderLine_sccfg, orderLine_nearCfg);
		// ----------------------------------------------------------------------------------------------------
		// ----------------------------------------------------------------------------------------------------
		// INITILIZE CACHE: item
		// ser: main config
		CacheConfiguration<Integer, Item> item_ccfg = new CacheConfiguration<Integer, Item>("item_ser");
		item_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		item_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction item_affFunc = new RendezvousAffinityFunction();
		item_affFunc.setExcludeNeighbors(true);
		item_affFunc.setPartitions(1);
		item_ccfg.setAffinity(item_affFunc);
		// stale config
		NearCacheConfiguration<Integer, Item> item_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<Integer, Item> item_sccfg = new CacheConfiguration<Integer, Item>("item_stale");
		item_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		item_sccfg.setCacheMode(CacheMode.REPLICATED);
		item_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(item_ccfg);
		ignite.createCache(item_sccfg, item_nearCfg);
		// ----------------------------------------------------------------------------------------------------
		// ----------------------------------------------------------------------------------------------------
		// INITILIZE CACHE: stock
		// ser: main config
		CacheConfiguration<DoubleKey, Stock> stock_ccfg = new CacheConfiguration<DoubleKey, Stock>("stock_ser");
		stock_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		stock_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction stock_affFunc = new RendezvousAffinityFunction();
		stock_affFunc.setExcludeNeighbors(true);
		stock_affFunc.setPartitions(1);
		stock_ccfg.setAffinity(stock_affFunc);
		// stale config
		NearCacheConfiguration<DoubleKey, Stock> stock_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<DoubleKey, Stock> stock_sccfg = new CacheConfiguration<DoubleKey, Stock>("stock_stale");
		stock_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		stock_sccfg.setCacheMode(CacheMode.REPLICATED);
		stock_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(stock_ccfg);
		ignite.createCache(stock_sccfg, stock_nearCfg);
		// ----------------------------------------------------------------------------------------------------
		// ----------------------------------------------------------------------------------------------------
		// INITILIZE CACHE: newOrder
		// ser: main config
		CacheConfiguration<TrippleKey, Boolean> newOrder_ccfg = new CacheConfiguration<TrippleKey, Boolean>(
				"newOrder_ser");
		newOrder_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		newOrder_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction newOrder_affFunc = new RendezvousAffinityFunction();
		newOrder_affFunc.setExcludeNeighbors(true);
		newOrder_affFunc.setPartitions(1);
		newOrder_ccfg.setAffinity(newOrder_affFunc);
		// stale config
		NearCacheConfiguration<TrippleKey, Boolean> newOrder_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<TrippleKey, Boolean> newOrder_sccfg = new CacheConfiguration<TrippleKey, Boolean>(
				"newOrder_stale");
		newOrder_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		newOrder_sccfg.setCacheMode(CacheMode.REPLICATED);
		newOrder_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(newOrder_ccfg);
		ignite.createCache(newOrder_sccfg, newOrder_nearCfg);
		// ----------------------------------------------------------------------------------------------------
		// ----------------------------------------------------------------------------------------------------
		// INITILIZE CACHE: history
		// ser: main config
		CacheConfiguration<Integer, History> history_ccfg = new CacheConfiguration<Integer, History>("history_ser");
		history_ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		history_ccfg.setCacheMode(CacheMode.REPLICATED);
		// ser: affinity config
		RendezvousAffinityFunction history_affFunc = new RendezvousAffinityFunction();
		history_affFunc.setExcludeNeighbors(true);
		history_affFunc.setPartitions(1);
		history_ccfg.setAffinity(history_affFunc);
		// stale config
		NearCacheConfiguration<Integer, History> history_nearCfg = new NearCacheConfiguration<>();
		CacheConfiguration<Integer, History> history_sccfg = new CacheConfiguration<Integer, History>("history_stale");
		history_sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		history_sccfg.setCacheMode(CacheMode.REPLICATED);
		history_sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
		// create both caches:
		ignite.createCache(history_ccfg);
		ignite.createCache(history_sccfg, history_nearCfg);
	}

	public void populateAllCaches(Ignite ignite, Constants cons) {
		IgniteCache<DoubleKey, District> district_cache = ignite.cache("district_ser");
		IgniteCache<DoubleKey, District> district_scache = ignite.cache("district_stale");
		IgniteCache<Integer, Warehouse> warehouse_cache = ignite.cache("warehouse_ser");
		IgniteCache<Integer, Warehouse> warehouse_scache = ignite.cache("warehouse_stale");
		IgniteCache<TrippleKey, Customer> customer_cache = ignite.cache("customer_ser");
		IgniteCache<TrippleKey, Customer> customer_scache = ignite.cache("customer_stale");
		IgniteCache<QuadKey, Order> order_cache = ignite.cache("order_ser");
		IgniteCache<QuadKey, Order> order_scache = ignite.cache("order_stale");
		IgniteCache<TrippleKey, OrderLine> orderLine_cache = ignite.cache("orderLine_ser");
		IgniteCache<TrippleKey, OrderLine> orderLine_scache = ignite.cache("orderLine_stale");
		IgniteCache<Integer, Item> item_cache = ignite.cache("item_ser");
		IgniteCache<Integer, Item> item_scache = ignite.cache("item_stale");
		IgniteCache<DoubleKey, Stock> stock_cache = ignite.cache("stock_ser");
		IgniteCache<DoubleKey, Stock> stock_scache = ignite.cache("stock_stale");
		IgniteCache<TrippleKey, Boolean> newOrder_cache = ignite.cache("newOrder_ser");
		IgniteCache<TrippleKey, Boolean> newOrder_scache = ignite.cache("newOrder_stale");
		IgniteCache<Integer, History> history_cache = ignite.cache("history_ser");
		IgniteCache<Integer, History> history_scache = ignite.cache("history_stale");
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
		// order
		for (QuadKey key : cons.all_keys_order) {
			System.out.println("init(order)" + key.toString());
			order_cache.put(key, new Order(0, "", false));
			order_scache.put(key, new Order(0, "", false));
		}
		// orderLine
		for (TrippleKey key : cons.all_keys_orderLine) {
			System.out.println("init(orderLine)" + key.toString());
			orderLine_cache.put(key, new OrderLine(0, 0, "", "", false));
			orderLine_scache.put(key, new OrderLine(0, 0, "", "", false));
		}
		// item
		for (int key : cons.all_keys_item) {
			String info = "I" + UUID.randomUUID().toString().substring(0, 5);
			System.out.println("init(item)" + key);
			item_cache.put(key, new Item(info, true));
			item_scache.put(key, new Item(info, true));
		}
		// stock
		for (DoubleKey key : cons.all_keys_stock) {
			System.out.println("init(stock)" + key.toString());
			int quant = ThreadLocalRandom.current().nextInt(20, 300);
			String info = "S" + UUID.randomUUID().toString().substring(0, 5);
			stock_cache.put(key, new Stock(0, quant, 0, info, true));
			stock_scache.put(key, new Stock(0, quant, 0, info, true));
		}
		// new order
		for (TrippleKey key : cons.all_keys_newOrder) {
			System.out.println("init(newOrder)" + key.toString());
			newOrder_cache.put(key, false);
			newOrder_scache.put(key, false);
		}
		// history
		for (int key : cons.all_keys_history) {
			String info = "H" + UUID.randomUUID().toString().substring(0, 5);
			System.out.println("init(history)" + key);
			history_cache.put(key, new History(info, true));
			history_scache.put(key, new History(info, true));
		}

	}

	public void printAll(Ignite ignite, Constants cons) {
		IgniteTransactions transactions = ignite.transactions();
		IgniteCache<DoubleKey, District> district_cache = ignite.cache("district_ser");
		IgniteCache<Integer, Warehouse> warehouse_cache = ignite.cache("warehouse_ser");
		IgniteCache<TrippleKey, Customer> customer_cache = ignite.cache("customer_ser");
		IgniteCache<QuadKey, Order> order_cache = ignite.cache("order_ser");
		IgniteCache<TrippleKey, OrderLine> orderLine_cache = ignite.cache("orderLine_ser");
		IgniteCache<Integer, Item> item_cache = ignite.cache("item_ser");
		IgniteCache<DoubleKey, Stock> stock_cache = ignite.cache("stock_ser");
		IgniteCache<TrippleKey, Boolean> newOrder_cache = ignite.cache("newOrder_ser");
		IgniteCache<Integer, History> history_cache = ignite.cache("history_ser");
		// distrcit
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<districts>>");
			System.out.println(
					"----------------------------------\nkey     	   value\n----------------------------------");
			for (DoubleKey key : cons.all_keys_district) {
				System.out.println("" + key.toString() + "	| " + district_cache.get(key).toString() + "");
			}
		}
		// warehouse
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<warehouses>>");
			System.out.println(
					"----------------------------------\nkey     	   value\n----------------------------------");
			for (int key : cons.all_keys_warehouse) {
				System.out.println("$(" + key + ")	| " + warehouse_cache.get(key).toString() + "");
			}
		}
		// customer
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<customer>>");
			System.out.println(
					"----------------------------------\nkey  	  	   value\n----------------------------------");
			for (TrippleKey key : cons.all_keys_customer) {
				System.out.println(key.toString() + "	| " + customer_cache.get(key).toString() + "");
			}
		}
		// order
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<order>>");
			System.out.println(
					"----------------------------------\nkey	   	   value\n----------------------------------");
			for (QuadKey key : cons.all_keys_order) {
				System.out.println(key.toString() + "	| " + order_cache.get(key).toString() + "");
			}
		}
		// orderLine
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<orderLine>>");
			System.out.println(
					"----------------------------------\nkey	   	   value\n----------------------------------");
			for (TrippleKey key : cons.all_keys_orderLine) {
				System.out.println(key.toString() + "	| " + orderLine_cache.get(key).toString() + "");
			}
		}
		// item
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<item>>");
			System.out.println(
					"----------------------------------\nkey	   	   value\n----------------------------------");
			for (int key : cons.all_keys_item) {
				System.out.println("$(" + key + ")" + "	| " + item_cache.get(key).toString() + "");
			}
		}
		// stock
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<stock>>");
			System.out.println(
					"----------------------------------\nkey	   	   value\n----------------------------------");
			for (DoubleKey key : cons.all_keys_stock) {
				System.out.println(key + "	| " + stock_cache.get(key).toString() + "");
			}
		}
		// new order
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<new order>>");
			System.out.println(
					"----------------------------------\nkey	   	   value\n----------------------------------");
			for (TrippleKey key : cons.all_keys_newOrder) {
				System.out.println(key + "	| " + newOrder_cache.get(key) + "");
			}
		}
		// history
		try (Transaction tx = transactions.txStart(cons.concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println("\n<<history>>");
			System.out.println(
					"----------------------------------\nkey	   	   value\n----------------------------------");
			for (int key : cons.all_keys_history) {
				System.out.println("$(" + key + ")" + "	| " + history_cache.get(key).toString() + "");
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
		IgniteCache<QuadKey, Order> order_cache = ignite.cache("order_ser");
		IgniteCache<QuadKey, Order> order_scache = ignite.cache("order_stale");
		IgniteCache<QuadKey, OrderLine> orderLine_cache = ignite.cache("orderLine_ser");
		IgniteCache<QuadKey, OrderLine> orderLine_scache = ignite.cache("orderLine_stale");
		IgniteCache<Integer, Item> item_cache = ignite.cache("item_ser");
		IgniteCache<Integer, Item> item_scache = ignite.cache("item_stale");
		IgniteCache<DoubleKey, Stock> stock_cache = ignite.cache("stock_ser");
		IgniteCache<DoubleKey, Stock> stock_scache = ignite.cache("stock_stale");
		IgniteCache<TrippleKey, Boolean> newOrder_cache = ignite.cache("newOrder_ser");
		IgniteCache<TrippleKey, Boolean> newOrder_scache = ignite.cache("newOrder_stale");
		IgniteCache<Integer, History> history_cache = ignite.cache("history_ser");
		IgniteCache<Integer, History> history_scache = ignite.cache("history_stale");
		district_cache.destroy();
		district_scache.destroy();
		warehouse_cache.destroy();
		warehouse_scache.destroy();
		customer_cache.destroy();
		customer_scache.destroy();
		order_cache.destroy();
		order_scache.destroy();
		orderLine_cache.destroy();
		orderLine_scache.destroy();
		item_cache.destroy();
		item_scache.destroy();
		stock_cache.destroy();
		stock_scache.destroy();
		newOrder_cache.destroy();
		newOrder_scache.destroy();
		history_cache.destroy();
		history_scache.destroy();
	}

}
