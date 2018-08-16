package com.mycompany.app;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.BasicAddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

public class Starter {
	public String localIp;
	public String publicIp;
	public String serverIp;

	public Starter(String localIp, String publicIp, String serverIp) {
		this.serverIp = serverIp;
		this.publicIp = publicIp;
		this.localIp = localIp;
	}

	public Ignite start() {
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setClientMode(true);
		// ADDRESS RESOLVER
		Map<String, String> addrMap = new HashMap<String, String>();
		addrMap.put(localIp, publicIp); // port forwarding
		BasicAddressResolver addrRes = null;
		try {
			addrRes = new BasicAddressResolver(addrMap);
		} catch (UnknownHostException e) {
			System.out.println(e);
		}
		cfg.setAddressResolver(addrRes);

		// IP FINDER
		TcpDiscoverySpi spi = new TcpDiscoverySpi();
		TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
		ipFinder.setAddresses(Arrays.asList(serverIp + ":47500..47509"));// Ohio Client
		spi.setIpFinder(ipFinder);
		cfg.setDiscoverySpi(spi);
		Ignite ignite = Ignition.start(cfg);
		return ignite;
	}

}
