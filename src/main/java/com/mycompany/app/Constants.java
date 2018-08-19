package com.mycompany.app;

import java.util.Set;
import java.util.TreeSet;

import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

public class Constants {
	public int _CLIENT_NUMBER;
	public int _DISTRICT_NUMBER;
	public int _WAREHOUSE_NUMBER;
	public int _CUSTOMER_NUMBER;
	public int _ITEM_NUMBER;
	public int _ORDER_NUMBER;
	public int _TOTAL_REPS;
	public int _ROUNDS;
	public int _HISTORY_NUMBER;

	Set<DoubleKey> all_keys_district;
	Set<Integer> all_keys_warehouse;
	Set<TrippleKey> all_keys_customer;
	Set<TrippleKey> all_keys_orderLine;
	Set<TrippleKey> all_keys_newOrder;
	Set<QuadKey> all_keys_order;
	Set<Integer> all_keys_item;
	Set<Integer> all_keys_history;
	Set<DoubleKey> all_keys_stock;

	TransactionIsolation ser;
	TransactionIsolation rc;
	TransactionConcurrency concurrency;

	public Constants(int clientNumber, int totals) {
		// change these later //TODO
		this._DISTRICT_NUMBER = 5;
		this._WAREHOUSE_NUMBER = 1;
		this._CUSTOMER_NUMBER = 20;
		this._ORDER_NUMBER = 30;
		this._HISTORY_NUMBER = 50;
		this._ITEM_NUMBER = 40;

		this._CLIENT_NUMBER = clientNumber;
		this._TOTAL_REPS = totals;
		this._ROUNDS = totals;// _TOTAL_REPS / _CLIENT_NUMBER;

		// create all district keys
		all_keys_district = new TreeSet<DoubleKey>();
		for (int d = 0; d < _DISTRICT_NUMBER; d++) {
			for (int w = 0; w < _WAREHOUSE_NUMBER; w++) {
				all_keys_district.add(new DoubleKey(d, w));
			}
		}
		// create all warehouse keys
		all_keys_warehouse = new TreeSet<Integer>();
		for (int w = 0; w < _WAREHOUSE_NUMBER; w++) {
			all_keys_warehouse.add(w);
		}
		// create all item keys
		all_keys_item = new TreeSet<Integer>();
		for (int i = 0; i < _ITEM_NUMBER; i++) {
			all_keys_item.add(i);
		}
		// create all customer keys
		all_keys_customer = new TreeSet<TrippleKey>();
		for (int c = 0; c < _CUSTOMER_NUMBER; c++)
			for (int d = 0; d < _DISTRICT_NUMBER; d++) {
				for (int w = 0; w < _WAREHOUSE_NUMBER; w++) {
					all_keys_customer.add(new TrippleKey(c, d, w));
				}
			}
		// create all orders keys
		all_keys_order = new TreeSet<QuadKey>();
		for (int o = 0; o < _ORDER_NUMBER; o++)
			for (int c = 0; c < _CUSTOMER_NUMBER; c++) {
				for (int d = 0; d < _DISTRICT_NUMBER; d++) {
					for (int w = 0; w < _WAREHOUSE_NUMBER; w++) {
						all_keys_order.add(new QuadKey(o, c, d, w));
					}
				}
			}
		// create all orderline keys
		all_keys_orderLine = new TreeSet<TrippleKey>();
		for (int o = 0; o < _ORDER_NUMBER; o++)
			for (int d = 0; d < _DISTRICT_NUMBER; d++) {
				for (int w = 0; w < _WAREHOUSE_NUMBER; w++) {
					all_keys_orderLine.add(new TrippleKey(o, d, w));
				}
			}
		// create all stock keys
		all_keys_stock = new TreeSet<DoubleKey>();
		for (int i = 0; i < _ITEM_NUMBER; i++) {
			for (int w = 0; w < _WAREHOUSE_NUMBER; w++) {
				all_keys_stock.add(new DoubleKey(i, w));
			}
		}
		// create all neworder keys
		all_keys_newOrder = new TreeSet<TrippleKey>();
		for (int o = 0; o < _ORDER_NUMBER; o++)
			for (int d = 0; d < _DISTRICT_NUMBER; d++) {
				for (int w = 0; w < _WAREHOUSE_NUMBER; w++) {
					all_keys_newOrder.add(new TrippleKey(o, d, w));
				}
			}
		// create all history keys
		all_keys_history = new TreeSet<Integer>();
		for (int h = 0; h < _HISTORY_NUMBER; h++) {
			all_keys_history.add(h);
		}

		ser = TransactionIsolation.SERIALIZABLE;
		rc = TransactionIsolation.READ_COMMITTED;
		concurrency = TransactionConcurrency.PESSIMISTIC;
	}
}
