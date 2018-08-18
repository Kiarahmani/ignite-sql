package com.mycompany.app;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;

public class Client {
	Runnable task;
	Thread threads[];
	static Stat[] myArray;
	private static AtomicReferenceArray<Stat> at;
	long clientsStartTime;
	long clientsFinishTime;
	Caches caches;

	public Caches announceReady(Ignite ignite, Constants cons) {
		int i = 0;
		IgniteCache<String, Integer> coordination_cache = ignite.cache("coordination");
		IgniteTransactions transactions = ignite.transactions();

		do {
			System.out.println("+++waiting for coordinator to initialize");
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (coordination_cache != null)
				i = coordination_cache.get("initialized");
			else
				coordination_cache = ignite.cache("coordination");
		} while (i != 1);
		// getting all the caches (it's better to do it once here)
		Caches caches = new Caches(ignite);
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			int v = coordination_cache.get("ready");
			coordination_cache.put("ready", v + 1);
			tx.commit();
			tx.close();
		}
		System.out.println("+++announced ready");
		return caches;

	}

	public void waitForAll(Ignite ignite, int followerCount) {
		IgniteCache<String, Integer> coordination_cache = ignite.cache("coordination");
		do {
			System.out.println("+++waiting for other followers to get ready");
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} while (coordination_cache.get("ready") < followerCount);
	}

	public void announceFinished(Ignite ignite, Constants cons) {
		IgniteCache<String, Integer> coordination_cache = ignite.cache("coordination");
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			int v = coordination_cache.get("finished");
			coordination_cache.put("finished", v + 1);
			tx.commit();
			tx.close();
		}
		System.out.println("+++announced finished");
	}

	// NEW ORDER (41%)
	public long payment(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.rc)) {
			tx.commit();
			tx.close();
		}
		//
		// System.out.println("doing payment");
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	// PAYMENT (41%)
	public long newOrder(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteCache<DoubleKey, District> district_cache = ignite.getOrCreateCache("district_ser");
		IgniteCache<TrippleKey, Customer> customer_cache = ignite.getOrCreateCache("customer_ser");
		IgniteCache<QuadKey, Order> order_cache = ignite.cache("order_ser");
		IgniteCache<Integer, Item> item_cache = ignite.cache("item_ser");
		IgniteCache<DoubleKey, Stock> stock_cache = ignite.cache("stock_ser");
		IgniteCache<TrippleKey, Boolean> newOrder_cache = ignite.cache("newOrder_ser");
		IgniteCache<Integer, Warehouse> warehouse_cache = ignite.cache("warehouse_ser");
		int wid = ThreadLocalRandom.current().nextInt(0, cons._WAREHOUSE_NUMBER);
		int did = ThreadLocalRandom.current().nextInt(0, cons._DISTRICT_NUMBER);
		int cid = ThreadLocalRandom.current().nextInt(0, cons._CUSTOMER_NUMBER);
		int item_count = ThreadLocalRandom.current().nextInt(5, 15);
		Set<Integer> item_keys = new TreeSet<Integer>();
		for (int i = 0; i < item_count; i++)
			item_keys.add(ThreadLocalRandom.current().nextInt(0, cons._ITEM_NUMBER));

		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {

			DoubleKey d_key = new DoubleKey(did, wid);
			TrippleKey c_key = new TrippleKey(cid, did, wid);
			// read district and warehouse tax rate
			int w_tax = warehouse_cache.get(wid).w_tax;
			District dist = district_cache.get(d_key);
			int d_tax = dist.d_tax;
			// update district's next order id
			district_cache.put(d_key,
					new District(dist.d_name, dist.d_address, dist.d_tax, dist.d_ytd, dist.d_nextoid + 1, true));
			// read the customer
			Customer cust = customer_cache.get(c_key);
			// insret a new order
			int carrier_id = ThreadLocalRandom.current().nextInt(0, 100);
			Order order = new Order(carrier_id, "08/18/2018", true);
			QuadKey order_key = new QuadKey(dist.d_nextoid + 1, cid, did, wid);
			TrippleKey newOrder_key = new TrippleKey(dist.d_nextoid + 1, did, wid);
			order_cache.put(order_key, order);
			newOrder_cache.put(newOrder_key, true);
			Map<Integer, Item> all_items = item_cache.getAll(item_keys);
			for (int i : item_keys) {
				// read the corresponding stock
				DoubleKey st_key = new DoubleKey(i, wid);
				int ol_quant = ThreadLocalRandom.current().nextInt(1, 11);
				Stock stck = stock_cache.get(st_key);
				// update the stock
				if (stck.s_quant - ol_quant > 10)
					stock_cache.put(st_key, new Stock(stck.s_ytd + ol_quant, stck.s_quant - ol_quant,
							stck.s_ordercnt + 1, stck.s_info, true));
				else
					stock_cache.put(st_key, new Stock(stck.s_ytd + ol_quant, stck.s_quant - ol_quant + 91,
							stck.s_ordercnt + 1, stck.s_info, true));
			}
			tx.commit();
			tx.close();
		}

		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	// DELIVERY (6%)
	public long delivery(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.rc)) {
			tx.commit();
			tx.close();
		}
		// System.out.println("doing delivery");
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	// STOCK_LEVEL (6%)
	public long stockLevel(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.rc)) {
			tx.commit();
			tx.close();
		}
		// System.out.println("doing stock level");
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	// ORDER_STATUS (6%)
	public long orderStatus(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			tx.commit();
			tx.close();
		}
		// System.out.println("doing order status");
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	public Client(Ignite ignite, Constants cons) {
		myArray = new Stat[cons._CLIENT_NUMBER * cons._ROUNDS];
		at = new AtomicReferenceArray<Stat>(myArray);
		task = new Runnable() {
			@Override
			public void run() {
				long estimatedTime = -1000000;
				String kind = "";
				int threadId = (int) (Thread.currentThread().getId() % cons._CLIENT_NUMBER);
				System.out.println("client #" + threadId + " started...");
				for (int rd = 0; rd < cons._ROUNDS; rd++) {
					int txn_type_rand = ThreadLocalRandom.current().nextInt(0, 100);
					if (txn_type_rand < 6) {
						kind = "os";
						estimatedTime = orderStatus(ignite, cons);
						System.out.println("tid-" + threadId + "(" + rd + ")----ORDRSTS(" + estimatedTime + "ms)");
					}
					if (txn_type_rand >= 6 && txn_type_rand < 12) {
						kind = "d";
						estimatedTime = delivery(ignite, cons);
						System.out.println("tid-" + threadId + "(" + rd + ")----DELIVRY(" + estimatedTime + "ms)");
					}
					if (txn_type_rand >= 12 && txn_type_rand < 18) {
						kind = "sl";
						estimatedTime = stockLevel(ignite, cons);
						System.out.println("tid-" + threadId + "(" + rd + ")----STCKLVL(" + estimatedTime + "ms)");
					}
					if (txn_type_rand >= 18 && txn_type_rand < 59) {
						kind = "p";
						estimatedTime = payment(ignite, cons);
						System.out.println("tid-" + threadId + "(" + rd + ")----PAYMENT(" + estimatedTime + "ms)");
					}
					if (txn_type_rand >= 59 && txn_type_rand < 100) {
						kind = "no";
						estimatedTime = newOrder(ignite, cons);
						System.out.println("tid-" + threadId + "(" + rd + ")----NEWORDR(" + estimatedTime + "ms)");
					}
					at.set(threadId * cons._ROUNDS + rd, new Stat(estimatedTime, kind));

				}

			}
		};

	}

	public void startAll(Caches caches, Constants cons) {
		this.caches = caches;
		// INITIATE CONCURRENT CLIENTS
		clientsStartTime = System.currentTimeMillis();
		threads = new Thread[cons._CLIENT_NUMBER];
		for (int i = 0; i < cons._CLIENT_NUMBER; i++) {
			threads[i] = new Thread(task);
			threads[i].start();
		}
	}

	public void joinAll(Constants cons) {
		for (int i = 0; i < cons._CLIENT_NUMBER; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				System.out.println(e);
			}
		}
		clientsFinishTime = System.currentTimeMillis();
	}

	public void printStats(Ignite ignite, Constants cons) {
		System.out.print("\n\n===========================================\n");
		long estimatedTime_tp = clientsFinishTime - clientsStartTime;
		System.out.println(ConsoleColors.YELLOW + "Throughput:"
				+ (cons._ROUNDS * cons._CLIENT_NUMBER) * 1000 / (estimatedTime_tp + 1) + " rounds/s"
				+ ConsoleColors.RESET);
		int sum_time = 0;
		int sum_time_delivery = 0, delivery_count = 0;
		int sum_time_newOrder = 0, newOrder_count = 0;
		int sum_time_stockLevel = 0, stockLevel_count = 0;
		int sum_time_orderStatus = 0, orderStatus_count = 0;
		int sum_time_payment = 0, payment_count = 0;
		for (int i = 0; i < cons._CLIENT_NUMBER * cons._ROUNDS; i++) {
			sum_time += at.get(i).latency;
			if (at.get(i).kind.equals("os")) {
				sum_time_orderStatus += at.get(i).latency;
				orderStatus_count++;
			}
			if (at.get(i).kind.equals("no")) {
				sum_time_newOrder += at.get(i).latency;
				newOrder_count++;
			}
			if (at.get(i).kind.equals("p")) {
				sum_time_payment += at.get(i).latency;
				payment_count++;
			}
			if (at.get(i).kind.equals("d")) {
				sum_time_delivery += at.get(i).latency;
				delivery_count++;
			}
			if (at.get(i).kind.equals("sl")) {
				sum_time_stockLevel += at.get(i).latency;
				stockLevel_count++;
			}
		}
		System.out.println(ConsoleColors.YELLOW + "Overall Latency:  "
				+ sum_time / (cons._CLIENT_NUMBER * (cons._ROUNDS)) + "ms" + ConsoleColors.RESET);
		// System.out.println(" |");
		if (newOrder_count != 0)
			System.out.println("    |----NwOrdr:  " + (sum_time_newOrder / newOrder_count) + "ms");
		if (payment_count != 0)
			System.out.println("    |----Pymnt:   " + (sum_time_payment / payment_count) + "ms");
		if (stockLevel_count != 0)
			System.out.println("    |----StckLvl: " + (sum_time_stockLevel / stockLevel_count) + "ms");
		if (orderStatus_count != 0)
			System.out.println("    |----OrdrSts: " + (sum_time_orderStatus / orderStatus_count) + "ms");
		if (delivery_count != 0)
			System.out.println("    |----Dlvry:   " + (sum_time_delivery / delivery_count) + "ms");

		System.out.print("===========================================\n\n\n\n");
	}

}
