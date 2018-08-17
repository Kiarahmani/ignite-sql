package com.mycompany.app;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;

public class Client {
	Runnable task;
	Thread threads[];
	static long[] myArray;
	private static AtomicLongArray at;
	long clientsStartTime;
	long clientsFinishTime;

	public long testTxn(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteTransactions transactions = ignite.transactions();
		IgniteCache<DoubleKey, District> district_cache = ignite.cache("district_ser");
		IgniteCache<DoubleKey, District> district_scache = ignite.cache("district_stale");
		IgniteCache<Integer, Warehouse> warehouse_cache = ignite.cache("warehouse_ser");
		IgniteCache<Integer, Warehouse> warehouse_scache = ignite.cache("warehouse_stale");
		// randomly pick a district and update its (and its warehouse's) ytd
		int w_id = ThreadLocalRandom.current().nextInt(0, cons._WAREHOUSE_NUMBER);
		int d_id = ThreadLocalRandom.current().nextInt(0, cons._DISTRICT_NUMBER);
		
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			// update w_ytd
			Warehouse wh = warehouse_cache.get(w_id);
			warehouse_cache.put(w_id, new Warehouse(wh.w_name, wh.w_address, wh.w_tax, wh.w_ytd + 1, true));
			tx.commit();
			tx.close();
		}

		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	public Client(Ignite ignite, Constants cons) {
		myArray = new long[cons._CLIENT_NUMBER * cons._ROUNDS];
		at = new AtomicLongArray(myArray);
		task = new Runnable() {
			@Override
			public void run() {
				long estimatedTime = -1000000;
				int threadId = (int) (Thread.currentThread().getId() % cons._CLIENT_NUMBER);
				System.out.println("client #" + threadId + " started...");
				for (int rd = 0; rd < cons._ROUNDS; rd++) {
					estimatedTime = testTxn(ignite, cons);
					at.set(threadId * cons._ROUNDS + rd, estimatedTime);
					System.out.println("#" + threadId + "(" + rd + "):"+estimatedTime+"ms");
				}

			}
		};

	}

	public void startAll(Constants cons) {
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
		System.out.println(
				"Throughput:" + (cons._ROUNDS * cons._CLIENT_NUMBER) * 1000 / (estimatedTime_tp + 1) + " rounds/s");
		int sum_time = 0;
		for (int i = 0; i < cons._CLIENT_NUMBER * cons._ROUNDS; i++) {
			sum_time += at.get(i);
		}
		System.out.println("Latency: " + sum_time / (cons._CLIENT_NUMBER * (cons._ROUNDS)) + "ms");
		System.out.print("===========================================\n\n\n\n");
	}

}
