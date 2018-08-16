// things are going smoothely!

package com.mycompany.app;

import java.util.*;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.*;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.*;

import com.mycompany.app.Starter;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.*;
import org.apache.ignite.configuration.BasicAddressResolver;
import org.apache.ignite.client.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.*;
import org.apache.ignite.cache.affinity.*;
import java.util.stream.Collectors;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.CachePeekMode;
import java.util.concurrent.ThreadLocalRandom;
import java.net.UnknownHostException;
import java.util.UUID;

public class App {
	public static final int _CLIENT_NUMBER = 8;
	public static final int _OBJECTS = 10;
	public static final int _ROUNDS = 100 / _CLIENT_NUMBER;
	static long[] myArray = new long[_CLIENT_NUMBER * _ROUNDS];
	private static AtomicLongArray at = new AtomicLongArray(myArray);

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		// fire ignite
		Ignite ignite = new Starter("172.31.19.186", "52.14.206.156", "54.169.147.152").start();
		
		IgniteTransactions transactions = ignite.transactions();
		System.out.println("All Available Caches on server : " + ignite.cacheNames());

		// INITILIZE A CACHE
		CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>("sync");
		ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
		ccfg.setCacheMode(CacheMode.PARTITIONED);
		ccfg.setBackups(1);
		ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
		// INITILIZE A CACHE
		CacheConfiguration<Integer, Integer> sccfg = new CacheConfiguration<Integer, Integer>("stale_sync");
		sccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		sccfg.setCacheMode(CacheMode.PARTITIONED);
		sccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

		// affinity setting
		RendezvousAffinityFunction affFunc = new RendezvousAffinityFunction();
		affFunc.setExcludeNeighbors(true);
		affFunc.setPartitions(1);
		ccfg.setAffinity(affFunc);
		// create cache
		IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(ccfg);
		IgniteCache<Integer, Integer> stale_cache = ignite.getOrCreateCache(sccfg);
		Set<Integer> all_keys = new TreeSet<Integer>();
		for (int i = 0; i < _OBJECTS; i++) {
			System.out.print(".");
			cache.put(i, 0);
			stale_cache.put(i, 1000000);
			all_keys.add(i);
		}

		TransactionIsolation ser = TransactionIsolation.SERIALIZABLE;
		TransactionIsolation rc = TransactionIsolation.READ_COMMITTED;
		TransactionConcurrency concurrency = TransactionConcurrency.PESSIMISTIC;
		// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
		// TESTS

		// PRINT STATS

		// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

		// PRINT THE FINAL VALUES
		int sum = 0;
		try (Transaction tx = transactions.txStart(concurrency, TransactionIsolation.SERIALIZABLE)) {
			System.out.println(">> final values:");
			int currentVal = 0;
			for (int i = 0; i < _OBJECTS; i++) {
				currentVal = cache.get(i);
				sum += currentVal;
				System.out.println("(" + i + "," + currentVal + ")");
			}
		}
		System.out.print("\n\n\n\n===========================================\n");
		System.out.println("\nSafety:  " + (_ROUNDS * _CLIENT_NUMBER == sum));
		// System.out.println( "Throuput:"+
		// ((_ROUNDS)*_CLIENT_NUMBER)*1000/estimatedTime_tp+" rounds/s");
		int sum_time = 0;
		for (int i = 0; i < _CLIENT_NUMBER * _ROUNDS; i++) {
			sum_time += at.get(i);
		}
		System.out.println("Latency: " + sum_time / (_CLIENT_NUMBER * (_ROUNDS)) + "ms");
		System.out.print("TOTAL EXECUTION TIME: " + (System.currentTimeMillis() - startTime) + "ms");
		cache.destroy();
		stale_cache.destroy();
		System.out.print("\n======================================\n\n\n\n");
	}
}
