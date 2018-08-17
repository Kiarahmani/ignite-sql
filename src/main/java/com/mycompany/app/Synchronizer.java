package com.mycompany.app;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

public class Synchronizer {
	Runnable task;

	public Synchronizer(Ignite ignite, Constants cons) {
		task = new Runnable() {
			@Override
			public void run() {
				IgniteCache<Integer, Integer> cache = ignite.cache("sync");
				IgniteCache<Integer, Integer> stale_cache = ignite.cache("stale_sync");
				try {
					while (true) {
						//for (int i = 0; i < cons._OBJECT_NUMBER; i++) {
							//stale_cache.put(i, cache.get(i));
						//}
						Thread.sleep(1000);
					}

				} catch (InterruptedException e) {
					System.out.println("Synchronizer stopped");
				}
			}
		};
	}
}
