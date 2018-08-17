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

	// NEW ORDER (41%)
	public long newOrder(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			tx.commit();
			tx.close();
		}
		System.out.println("doing newOrder");
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	// PAYMENT (41%)
	public long payment(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			tx.commit();
			tx.close();
		}
		System.out.println("doing payment");
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	// DELIVERY (6%)
	public long delivery(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			tx.commit();
			tx.close();
		}
		System.out.println("doing delivery");
		long estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}

	// STOCK_LEVEL (6%)
	public long stockLevel(Ignite ignite, Constants cons) {
		long startTime = System.currentTimeMillis();
		IgniteTransactions transactions = ignite.transactions();
		try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
			tx.commit();
			tx.close();
		}
		System.out.println("doing stock level");
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
		System.out.println("doing order status");
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
					int txn_type_rand = ThreadLocalRandom.current().nextInt(0, 100);
					if (txn_type_rand < 6)
						estimatedTime = orderStatus(ignite, cons);
					if (txn_type_rand >= 6 && txn_type_rand < 12)
						estimatedTime = delivery(ignite, cons);
					if (txn_type_rand >= 12 && txn_type_rand < 18)
						estimatedTime = stockLevel(ignite, cons);
					if (txn_type_rand >= 18 && txn_type_rand < 59)
						estimatedTime = payment(ignite, cons);
					if (txn_type_rand >= 59 && txn_type_rand < 100)
						estimatedTime = newOrder(ignite, cons);
					at.set(threadId * cons._ROUNDS + rd, estimatedTime);
					System.out.println("#" + threadId + "(" + rd + "):" + estimatedTime + "ms");
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
