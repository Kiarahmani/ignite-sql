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

	public void testTxn(Ignite ignite) {
		IgniteTransactions transactions = ignite.transactions();
		IgniteCache<DoubleKey, District> district_cache = ignite.cache("district_ser");
		IgniteCache<DoubleKey, District> district_scache = ignite.cache("district_stale");
		IgniteCache<DoubleKey, District> warehouse_cache = ignite.cache("district_ser");
		IgniteCache<DoubleKey, District> warehouse_scache = ignite.cache("district_stale");
		System.out.println("doing some shitty tasks");
	}

	public Client(Ignite ignite, Constants cons) {
		myArray = new long[cons._CLIENT_NUMBER * cons._ROUNDS];
		at = new AtomicLongArray(myArray);
		task = new Runnable() {
			@Override
			public void run() {
				int threadId = (int) (Thread.currentThread().getId() % cons._CLIENT_NUMBER);
				System.out.println("client #" + threadId + " started...");
				for (int rd = 0; rd < cons._ROUNDS; rd++) {
					testTxn(ignite);
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
