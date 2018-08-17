// aug 16

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
	public static final boolean _COORDINATOR = false;
	public static final int _FOLLOWER_COUNT = 1;

	public static void main(String[] args) {
		System.out.println(args[0]);
		Constants cons = new Constants(2, 10);
		Ignite ignite = new Starter("172.31.19.186", "18.222.69.139", /* server */"18.136.120.183").start();
		CacheManager manager = new CacheManager(ignite);
		Client clients = new Client(ignite, cons);
		if (_COORDINATOR) {
			manager.createCoordinationCaches(ignite, _FOLLOWER_COUNT);
			manager.createAllCaches(ignite);
			manager.populateAllCaches(ignite, cons);
			manager.dispatchFollowers(ignite);
			manager.waitForFollowers(ignite, _FOLLOWER_COUNT);
			manager.printAll(ignite, cons);
			manager.destroyAll(ignite, cons);
		} else {

			System.out.print("\n\n\n\nTXN EXECUTION" + "\n===========================================\n");
			clients.announceReady(ignite, cons);
			clients.waitForAll(ignite, _FOLLOWER_COUNT);
			clients.startAll(cons);
			clients.joinAll(cons);
			clients.printStats(ignite, cons);
			clients.announceFinished(ignite, cons);
		}

	}
}
