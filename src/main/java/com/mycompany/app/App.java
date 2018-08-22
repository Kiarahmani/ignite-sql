// aug 2

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

	public static void main(String[] args) {
		boolean _COORDINATOR = Boolean.valueOf(args[0]);
		int _FOLLOWER_COUNT = Integer.valueOf(args[1]);
		int _CLIENT_NUMBER = Integer.valueOf(args[2]);
		System.out.println("Client NUmbers: " + _CLIENT_NUMBER);
		Constants cons = new Constants(/* clients */ _CLIENT_NUMBER, /* totals */ 256, /* size */ 10, false);
		Starter starter = null;
		if (_COORDINATOR) {
			starter = new Starter("18.222.125.148", "18.222.69.139", /* server */"107.23.89.220");
		} else {
			starter = new Starter("18.222.125.148", "18.222.69.139", /* server */"18.219.198.133");
		}
		Ignite ignite = starter.start();

		if (_COORDINATOR) {
			CacheManager manager = new CacheManager(ignite);
			manager.createCoordinationCaches(ignite, _FOLLOWER_COUNT);
			manager.createAllCaches(ignite);
			manager.populateAllCaches(ignite, cons);
			manager.dispatchFollowers(ignite);
			manager.waitForFollowers(ignite, _FOLLOWER_COUNT);
			// manager.printAll(ignite, cons);
			manager.destroyAll(ignite, cons);
			starter.stop();
		} else {
			if (!cons._CHOPPED) {
				Client clients = new Client(ignite, cons);
				System.out.print("\n\n\n\nTXN EXECUTION" + "\n===========================================\n");
				Caches caches = clients.announceReady(ignite, cons);
				clients.waitForAll(ignite, _FOLLOWER_COUNT);
				clients.startAll(caches, cons);
				clients.joinAll(cons);
				clients.printStats(ignite, cons);
				clients.announceFinished(ignite, cons);
				starter.stop();
			} else {
				ChoppedClient clients = new ChoppedClient(ignite, cons);
				System.out.print("\n\n\n\nTXN EXECUTION" + "\n===========================================\n");
				Caches caches = clients.announceReady(ignite, cons);
				clients.waitForAll(ignite, _FOLLOWER_COUNT);
				clients.startAll(caches, cons);
				clients.joinAll(cons);
				clients.printStats(ignite, cons);
				clients.announceFinished(ignite, cons);
				starter.stop();
			}
		}

	}
}
