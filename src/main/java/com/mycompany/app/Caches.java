package com.mycompany.app;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

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

	IgniteCache<TrippleKey, OrderLine> orderLine_cache;
	IgniteCache<TrippleKey, OrderLine> orderLine_scache;

	public Caches(Ignite ignite) {
		this.warehouse_cache = ignite.cache("warehouse_ser");
		this.warehouse_scache = ignite.cache("warehouse_stale");

		this.district_cache = ignite.cache("district_ser");
		this.district_scache = ignite.cache("district_stale");

		this.customer_cache = ignite.cache("customer_ser");
		this.customer_scache = ignite.cache("customer_stale");

		this.order_cache = ignite.cache("order_ser");
		this.order_scache = ignite.cache("order_stale");

		this.item_cache = ignite.cache("item_ser");
		this.item_scache = ignite.cache("item_stale");

		this.stock_cache = ignite.cache("stock_ser");
		this.stock_scache = ignite.cache("stock_stale");

		this.newOrder_cache = ignite.cache("newOrder_ser");
		this.newOrder_scache = ignite.cache("newOrder_stale");

		this.history_cache = ignite.cache("history_ser");
		this.history_scache = ignite.cache("history_stale");

		this.orderLine_cache = ignite.cache("orderLine_ser");
		this.orderLine_scache = ignite.cache("orderLine_stale");

	}

}
