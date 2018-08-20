package com.mycompany.app;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
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

	//////////////////
	// PAYMENT (41%)
	public long payment(Ignite ignite, Constants cons) {

		long startTime = System.currentTimeMillis();
		int wid = ThreadLocalRandom.current().nextInt(0, cons._WAREHOUSE_NUMBER);
		int did = ThreadLocalRandom.current().nextInt(0, cons._DISTRICT_NUMBER);
		DoubleKey d_key = new DoubleKey(did, wid);
		int h_amount = ThreadLocalRandom.current().nextInt(1, 5001);
		String h_info = "H" + UUID.randomUUID().toString().substring(0, 15);
		boolean byLastName = (ThreadLocalRandom.current().nextInt(0, 100) > 40); // 60% chance of query by last name
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			// update w_ytd
			Warehouse wh = caches.warehouse_scache.get(wid);
			caches.warehouse_cache.put(wid,
					new Warehouse(wh.w_name, wh.w_address, wh.w_tax, wh.w_ytd + h_amount, true));
			// update d_ytd
			District dist = caches.district_cache.get(d_key);
			caches.district_cache.put(d_key,
					new District(dist.d_name, dist.d_address, dist.d_tax, dist.d_ytd + h_amount, dist.d_nextoid, true));
			// update custmer 40%(60%) of the time by id (last name)
			if (byLastName) {
				String givenLastName = UUID.randomUUID().toString().substring(0, 1);
				// create a local set of keys for the current w_id and d_id
				Set<TrippleKey> partial_cust_keys = new TreeSet<TrippleKey>();
				for (TrippleKey k : cons.all_keys_customer)
					if (k.k2 == did && k.k3 == wid)
						partial_cust_keys.add(k);
				// fetch all such custemrs
				Map<TrippleKey, Customer> filtered_custs = caches.customer_cache.getAll(partial_cust_keys);
				// filter them based on the current last name
				TrippleKey chosen_key = null;
				Customer chosen_cust = null;
				for (TrippleKey k : filtered_custs.keySet())
					if (filtered_custs.get(k).c_name.contains(givenLastName)) {
						chosen_key = k;
						chosen_cust = filtered_custs.get(k);
					}
				// update the chosen customer
				if (chosen_cust != null)
					caches.customer_cache.put(chosen_key,
							new Customer(chosen_cust.c_name, chosen_cust.c_address, chosen_cust.c_balance - h_amount,
									chosen_cust.c_discount, chosen_cust.c_credit, chosen_cust.c_payment_count + 1,
									chosen_cust.c_ytd + h_amount, chosen_cust.c_deliverycnt, true));

			} else {
				int cid = ThreadLocalRandom.current().nextInt(0, cons._CUSTOMER_NUMBER);
				TrippleKey c_key = new TrippleKey(cid, did, wid);
				Customer c = caches.customer_cache.get(c_key);
				caches.customer_cache.put(c_key, new Customer(c.c_name, c.c_address, c.c_balance - h_amount,
						c.c_discount, c.c_credit, c.c_payment_count + 1, c.c_ytd + h_amount, c.c_deliverycnt, true));
			}
			tx.commit();
			tx.close();
		}

		//
		// System.out.println("doing payment");
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	//////////////////
	// NEW ORDER (41%)
	public long newOrder(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		int wid = ThreadLocalRandom.current().nextInt(0, cons._WAREHOUSE_NUMBER);
		int did = ThreadLocalRandom.current().nextInt(0, cons._DISTRICT_NUMBER);
		int cid = ThreadLocalRandom.current().nextInt(0, cons._CUSTOMER_NUMBER);
		int item_count = ThreadLocalRandom.current().nextInt(5, 15);
		Set<Integer> item_keys = new TreeSet<Integer>();
		Set<DoubleKey> stock_keys = new TreeSet<DoubleKey>();
		Set<QuadKey> orderLine_keys = new TreeSet<QuadKey>();
		for (int i = 0; i < item_count; i++) {
			int iRand = ThreadLocalRandom.current().nextInt(0, cons._ITEM_NUMBER);
			DoubleKey skey = new DoubleKey(iRand, wid);
			item_keys.add(iRand);
			stock_keys.add(skey);
		}

		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {

			DoubleKey d_key = new DoubleKey(did, wid);
			TrippleKey c_key = new TrippleKey(cid, did, wid);
			// read district and warehouse tax rate
			int w_tax = caches.warehouse_cache.get(wid).w_tax;
			District dist = caches.district_cache.get(d_key);
			int d_tax = dist.d_tax;
			// update district's next order id
			caches.district_cache.put(d_key,
					new District(dist.d_name, dist.d_address, dist.d_tax, dist.d_ytd, dist.d_nextoid + 1, true));
			// read the customer
			Customer cust = caches.customer_cache.get(c_key);
			// insret a new order
			int carrier_id = ThreadLocalRandom.current().nextInt(0, 100);
			Order order = new Order(cid, carrier_id, "08/18/2018", true);
			TrippleKey order_key = new TrippleKey(dist.d_nextoid + 1, did, wid);
			// create all partial orderLine keys for this order
			for (int i = 0; i < cons._ORDERLINE_NUMBER; i++)
				orderLine_keys.add(new QuadKey(i, order_key.k1, did, wid));
			TrippleKey newOrder_key = new TrippleKey(dist.d_nextoid + 1, did, wid);
			caches.order_cache.put(order_key, order);
			caches.newOrder_cache.put(newOrder_key, true);
			Map<Integer, Item> all_items = caches.item_cache.getAll(item_keys);
			Map<DoubleKey, Stock> all_stocks = caches.stock_cache.getAll(stock_keys);
			Map<QuadKey, OrderLine> all_orderLines = caches.orderLine_cache.getAll(orderLine_keys);
			int ol_number = 0;
			for (DoubleKey st_key : all_stocks.keySet()) {
				// insert a new orderLine
				all_orderLines.put(new QuadKey(ol_number, order_key.k1, did, wid),
						new OrderLine(st_key.k1, "", "S_DIST_" + String.valueOf(did), true));
				ol_number++;
				// read the corresponding stock
				int ol_quant = ThreadLocalRandom.current().nextInt(1, 11);
				Stock stck = all_stocks.get(st_key);
				// update the stock
				if (stck.s_quant - ol_quant > 10)
					all_stocks.put(st_key, new Stock(stck.s_ytd + ol_quant, stck.s_quant - ol_quant,
							stck.s_ordercnt + 1, stck.s_info, true));
				else
					all_stocks.put(st_key, new Stock(stck.s_ytd + ol_quant, stck.s_quant - ol_quant + 91,
							stck.s_ordercnt + 1, stck.s_info, true));
			}
			caches.orderLine_cache.putAll(all_orderLines);
			caches.stock_cache.putAll(all_stocks);
			tx.commit();
			tx.close();
		}

		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	// DELIVERY (6%)
	public long delivery(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		int wid = ThreadLocalRandom.current().nextInt(0, cons._WAREHOUSE_NUMBER);
		int did = ThreadLocalRandom.current().nextInt(0, cons._DISTRICT_NUMBER);
		Set<TrippleKey> partial_newOrder_keys = new TreeSet<TrippleKey>();
		// all orders from this w_id and d_id must be fetched
		for (TrippleKey k : cons.all_keys_newOrder)
			if (k.k2 == did && k.k3 == wid)
				partial_newOrder_keys.add(k);
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			Map<TrippleKey, Boolean> partial_newOrders = caches.newOrder_cache.getAll(partial_newOrder_keys);
			// pick the oldest order
			int oldest_oid = cons._ORDER_NUMBER;
			TrippleKey selected_no_key = null;
			for (TrippleKey k : partial_newOrders.keySet())
				if (k.k1 < oldest_oid && partial_newOrders.get(k)) {
					oldest_oid = k.k1;
					selected_no_key = k;
				}
			// delete the selected new_order
			if (selected_no_key != null)
				caches.newOrder_cache.put(selected_no_key, false);
			// select the matching order
			Order selected_order = caches.order_cache.get(selected_no_key);
			int cid = selected_order.o_cid;
			// update the carrier_id of the selected order
			int car_id = ThreadLocalRandom.current().nextInt(0, 5);
			caches.order_cache.put(selected_no_key, new Order(cid, car_id, selected_order.o_entry_date, true));
			// select all matching orderline rows
			Set<QuadKey> partial_orderLine_keys = new TreeSet<>();
			for (QuadKey k : cons.all_keys_orderLine)
				if (k.k2 == selected_no_key.k1 && k.k3 == did && k.k4 == cid)
					partial_orderLine_keys.add(k);
			Map<QuadKey, OrderLine> partial_orderLines = caches.orderLine_cache.getAll(partial_orderLine_keys);
			// sum of ol_amount is retrieved and delivery_date is updated
			int sum_ol_amount = 0;
			for (QuadKey k : partial_orderLines.keySet()) {
				sum_ol_amount += 1;
				OrderLine ol = partial_orderLines.get(k);
				partial_orderLines.put(k, new OrderLine(ol.ol_iid, "08/20/2018", ol.ol_info, true));
			}
			caches.orderLine_cache.putAll(partial_orderLines);
			// update the matching customer
			TrippleKey c_key = new TrippleKey(cid, did, wid);
			Customer cust = caches.customer_cache.get(c_key);
			caches.customer_cache.put(c_key, new Customer(cust.c_name, cust.c_address, cust.c_balance - sum_ol_amount,
					cust.c_discount, cust.c_credit, cust.c_payment_count, cust.c_ytd, cust.c_deliverycnt + 1, true));
			tx.commit();
			tx.close();
		}
		// System.out.println("doing delivery");
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	//////////////////
	// ORDER_STATUS (6%)
	public long orderStatus(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		int wid = ThreadLocalRandom.current().nextInt(0, cons._WAREHOUSE_NUMBER);
		int did = ThreadLocalRandom.current().nextInt(0, cons._DISTRICT_NUMBER);
		TrippleKey chosen_key = null;
		boolean byLastName = (ThreadLocalRandom.current().nextInt(0, 100) > 40); // 60% chance of query by last name
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			// pick the customer either by last name or the id
			if (byLastName) {
				String givenLastName = UUID.randomUUID().toString().substring(0, 1);
				// create a local set of keys for the current w_id and d_id
				Set<TrippleKey> partial_cust_keys = new TreeSet<TrippleKey>();
				for (TrippleKey k : cons.all_keys_customer)
					if (k.k2 == did && k.k3 == wid)
						partial_cust_keys.add(k);
				// fetch all such custemrs
				Map<TrippleKey, Customer> filtered_custs = caches.customer_cache.getAll(partial_cust_keys);
				// filter them based on the current last name
				Customer chosen_cust;
				for (TrippleKey k : filtered_custs.keySet())
					if (filtered_custs.get(k).c_name.contains(givenLastName)) {
						chosen_key = k;
						chosen_cust = filtered_custs.get(k);
						break;
					}
			} else {
				chosen_key = new TrippleKey(ThreadLocalRandom.current().nextInt(0, cons._CUSTOMER_NUMBER), did, wid);
				Customer chosen_cust = caches.customer_cache.get(chosen_key);
			}
			// query orders based on the chosen customer (if exists)
			if (chosen_key != null) {
				Set<TrippleKey> partial_order_keys = new TreeSet<TrippleKey>();
				for (TrippleKey k : cons.all_keys_order)
					if (k.k2 == did && k.k3 == wid)
						partial_order_keys.add(k);

				Map<TrippleKey, Order> filtered_ords = caches.order_cache.getAll(partial_order_keys);
				// pick the order with the largest o_id
				Order chosen_ord;
				TrippleKey chosen_oid;
				int max_key = 0;
				for (TrippleKey k : filtered_ords.keySet())
					if (filtered_ords.get(k).isAlive && k.k1 > max_key && filtered_ords.get(k).o_cid == chosen_key.k1) {
						max_key = k.k1;
						chosen_oid = k;
						chosen_ord = filtered_ords.get(k);
					}
			}
			tx.commit();
			tx.close();
		}
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	//////////////////
	// STOCK_LEVEL (6%)
	public long stockLevel(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		int wid = ThreadLocalRandom.current().nextInt(0, cons._WAREHOUSE_NUMBER);
		int did = ThreadLocalRandom.current().nextInt(0, cons._DISTRICT_NUMBER);
		int threshold = ThreadLocalRandom.current().nextInt(10, 21);
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			District dist = caches.district_cache.get(new DoubleKey(did, wid));
			Set<QuadKey> partial_orderLine_keys = new TreeSet<QuadKey>();
			Set<DoubleKey> filtered_stock_keys = new TreeSet<DoubleKey>();
			for (QuadKey k : cons.all_keys_orderLine) {
				if (k.k2 == did && k.k3 == wid && k.k1 <= dist.d_nextoid && k.k1 > (dist.d_nextoid - 20))
					partial_orderLine_keys.add(k);
			}
			Map<QuadKey, OrderLine> filtered_orderLines = caches.orderLine_cache.getAll(partial_orderLine_keys);
			// get stocks and filter them according to the threshold
			for (OrderLine o : filtered_orderLines.values())
				filtered_stock_keys.add(new DoubleKey(o.ol_iid, wid));
			Map<DoubleKey, Stock> filtered_stocks = caches.stock_cache.getAll(filtered_stock_keys);
			Set<Stock> final_stocks = new HashSet<Stock>();
			for (Stock s : filtered_stocks.values())
				if (s.s_quant < threshold)
					final_stocks.add(s);
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
						System.out.println(
								"tid-" + threadId + "(" + rd + ")----ORDRSTS(" + estimatedTime / 200 + " rtt)");
					}
					if (txn_type_rand >= 6 && txn_type_rand < 12) {
						kind = "d";
						estimatedTime = delivery(ignite, cons);
						System.out.println(
								"tid-" + threadId + "(" + rd + ")----DELIVRY(" + estimatedTime / 200 + " rtt)");
					}
					if (txn_type_rand >= 12 && txn_type_rand < 18) {
						kind = "sl";
						estimatedTime = stockLevel(ignite, cons);
						System.out.println(
								"tid-" + threadId + "(" + rd + ")----STCKLVL(" + estimatedTime / 200 + " rtt)");
					}
					if (txn_type_rand >= 18 && txn_type_rand < 59) {
						kind = "p";
						estimatedTime = payment(ignite, cons);
						System.out.println(
								"tid-" + threadId + "(" + rd + ")----PAYMENT(" + estimatedTime / 200 + " rtt)");
					}
					if (txn_type_rand >= 59 && txn_type_rand < 100) {
						kind = "no";
						estimatedTime = newOrder(ignite, cons);
						System.out.println(
								"tid-" + threadId + "(" + rd + ")----NEWORDR(" + estimatedTime / 200 + " rtt)");
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
