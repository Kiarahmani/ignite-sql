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
	public static void main(String[] args) {
		Constants cons = new Constants(1, 10, 100);
		Ignite ignite = new Starter("172.31.19.186", "18.222.69.139", "54.251.179.146").start();
		CacheManager manager = new CacheManager(ignite);
		Client clients = new Client(ignite, cons);
		manager.createAllCaches(ignite);
		manager.populateAllCaches(ignite, cons);

		// START CLIENTS
		clients.startAll(cons);
		// WAIT FOR ALL CLEINTS
		clients.joinAll(cons);
		// PRINT STATS
		clients.printStats(ignite, cons);
		// PRINT THE FINAL VALUES
		manager.printAll(ignite, cons);
		// DESTROYCACHES
		manager.destroyAll(ignite, cons);
		
	}
}
