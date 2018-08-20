package com.mycompany.app;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.NearCacheConfiguration;

public class Caches {
	IgniteCache<Integer, Warehouse> warehouse_cache;
	IgniteCache<Integer, Warehouse> warehouse_scache;

	IgniteCache<DoubleKey, District> district_cache;
	IgniteCache<DoubleKey, District> district_scache;

	IgniteCache<TrippleKey, Customer> customer_cache;
	IgniteCache<TrippleKey, Customer> customer_scache;

	IgniteCache<QuadKey, Order> order_cache;
	IgniteCache<QuadKey, Order> order_scache;

	IgniteCache<Integer, Item> item_cache;
	IgniteCache<Integer, Item> item_scache;

	IgniteCache<DoubleKey, Stock> stock_cache;
	IgniteCache<DoubleKey, Stock> stock_scache;

	IgniteCache<TrippleKey, Boolean> newOrder_cache;
	IgniteCache<TrippleKey, Boolean> newOrder_scache;

	IgniteCache<Integer, History> history_cache;
	IgniteCache<Integer, History> history_scache;

	IgniteCache<QuadKey, OrderLine> orderLine_cache;
	IgniteCache<QuadKey, OrderLine> orderLine_scache;

	public Caches(Ignite ignite) {
		NearCacheConfiguration<Integer, Warehouse> warehouse_nearCfg = new NearCacheConfiguration<Integer, Warehouse>();
		this.warehouse_cache = ignite.cache("warehouse_ser");
		this.warehouse_scache = ignite.getOrCreateNearCache("warehouse_stale", warehouse_nearCfg);

		NearCacheConfiguration<DoubleKey, District> district_nearCfg = new NearCacheConfiguration<DoubleKey, District>();
		this.district_cache = ignite.cache("district_ser");
		this.district_scache = ignite.getOrCreateNearCache("district_stale", district_nearCfg);

		NearCacheConfiguration<TrippleKey, Customer> customer_nearCfg = new NearCacheConfiguration<TrippleKey, Customer>();
		this.customer_cache = ignite.cache("customer_ser");
		this.customer_scache = ignite.getOrCreateNearCache("customer_stale", customer_nearCfg);

		NearCacheConfiguration<QuadKey, Order> order_nearCfg = new NearCacheConfiguration<QuadKey, Order>();
		this.order_cache = ignite.cache("order_ser");
		this.order_scache = ignite.getOrCreateNearCache("order_stale", order_nearCfg);

		NearCacheConfiguration<Integer, Item> item_nearCfg = new NearCacheConfiguration<Integer, Item>();
		this.item_cache = ignite.cache("item_ser");
		this.item_scache = ignite.getOrCreateNearCache("item_stale", item_nearCfg);

		NearCacheConfiguration<DoubleKey, Stock> stock_nearCfg = new NearCacheConfiguration<DoubleKey, Stock>();
		this.stock_cache = ignite.cache("stock_ser");
		this.stock_scache = ignite.getOrCreateNearCache("stock_stale", stock_nearCfg);

		NearCacheConfiguration<TrippleKey, Boolean> newOrder_nearCfg = new NearCacheConfiguration<TrippleKey, Boolean>();
		this.newOrder_cache = ignite.cache("newOrder_ser");
		this.newOrder_scache = ignite.getOrCreateNearCache("newOrder_stale", newOrder_nearCfg);

		NearCacheConfiguration<Integer, History> history_nearCfg = new NearCacheConfiguration<Integer, History>();
		this.history_cache = ignite.cache("history_ser");
		this.history_scache = ignite.getOrCreateNearCache("history_stale", history_nearCfg);

		NearCacheConfiguration<QuadKey, OrderLine> orderLine_nearCfg = new NearCacheConfiguration<QuadKeyw, OrderLine>();
		this.orderLine_cache = ignite.cache("orderLine_ser");
		this.orderLine_scache = ignite.getOrCreateNearCache("orderLine_stale", orderLine_nearCfg);

	}

}
