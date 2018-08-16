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

	public Client(Ignite ignite, Constants cons) {
		myArray = new long[cons._CLIENT_NUMBER * cons._ROUNDS];
		at = new AtomicLongArray(myArray);
		task = new Runnable() {
			@Override
			public void run() {
				IgniteTransactions transactions = ignite.transactions();
				IgniteCache<Integer, Integer> cache = ignite.cache("sync");
				IgniteCache<Integer, Integer> stale_cache = ignite.cache("stale_sync");

				int threadId = (int) (Thread.currentThread().getId() % cons._CLIENT_NUMBER);
				System.out.println("client #" + threadId + " started...");
				for (int i = 0; i < cons._ROUNDS; i++) {
					long txnStartTime = System.currentTimeMillis();
					int key = ThreadLocalRandom.current().nextInt(0, cons._OBJECT_NUMBER);
					int value = -1000000;
					int kir = stale_cache.get(1);
					try (Transaction tx = transactions.txStart(cons.concurrency, cons.ser)) {
						//value = kvMap.get(key);
						//cache.put(key, value + 2);
						tx.commit();
						tx.close();
					}
					long estimatedTime = System.currentTimeMillis() - txnStartTime;
					System.out.println(estimatedTime);
					at.set(threadId * cons._ROUNDS + i, estimatedTime);
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
		System.out.print("\n\n\n\n===========================================\n");
		long estimatedTime_tp = clientsFinishTime - clientsStartTime;
		System.out
				.println("Throughput:" + (cons._ROUNDS * cons._CLIENT_NUMBER) * 1000 / estimatedTime_tp + " rounds/s");
		int sum_time = 0;
		for (int i = 0; i < cons._CLIENT_NUMBER * cons._ROUNDS; i++) {
			sum_time += at.get(i);
		}
		System.out.println("Latency: " + sum_time / (cons._CLIENT_NUMBER * (cons._ROUNDS)) + "ms");
		System.out.print("===========================================\n\n\n\n");
	}

}
