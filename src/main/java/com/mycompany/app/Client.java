package com.mycompany.app;

import java.util.Map;
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
					}
					if (txn_type_rand >= 6 && txn_type_rand < 12) {
						kind = "d";
						estimatedTime = delivery(ignite, cons);
					}
					if (txn_type_rand >= 12 && txn_type_rand < 18) {
						kind = "sl";
						estimatedTime = stockLevel(ignite, cons);
					}
					if (txn_type_rand >= 18 && txn_type_rand < 59) {
						kind = "p";
						estimatedTime = payment(ignite, cons);
					}
					if (txn_type_rand >= 59 && txn_type_rand < 100) {
						kind = "no";
						estimatedTime = newOrder(ignite, cons);
					}
					at.set(threadId * cons._ROUNDS + rd, new Stat(estimatedTime, kind));
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
		System.out.println("Overall Latency: " + sum_time / (cons._CLIENT_NUMBER * (cons._ROUNDS)) + "ms");
		System.out.println("      New-Order: " + (sum_time_newOrder / newOrder_count) + "ms");
		System.out.println("        Payment: " + (sum_time_payment / payment_count) + "ms");
		System.out.println("    Stock Level: " + (sum_time_stockLevel / stockLevel_count) + "ms");
		System.out.println("   Order Status: " + (sum_time_orderStatus / orderStatus_count) + "ms");
		System.out.println("       Delivery: " + (sum_time_delivery / delivery_count) + "ms");

		System.out.print("===========================================\n\n\n\n");
	}

}
