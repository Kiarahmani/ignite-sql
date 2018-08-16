package com.mycompany.app;

import java.util.Set;
import java.util.TreeSet;

import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

public class Constants {
	public int _CLIENT_NUMBER;
	public int _OBJECT_NUMBER;
	public int _DISTRICT_NUMBER;
	public int _WAREHOUSE_NUMBER;
	public int _TOTAL_REPS;
	public int _ROUNDS;
	Set<Integer> all_keys;
	Set<DoubleKey> all_keys_district;

	TransactionIsolation ser;
	TransactionIsolation rc;
	TransactionConcurrency concurrency;

	public Constants(int clientNumber, int objectNumber, int totals) {
		// change these later //TODO
		this._DISTRICT_NUMBER = 15;
		this._WAREHOUSE_NUMBER = 10;

		this._CLIENT_NUMBER = clientNumber;
		this._OBJECT_NUMBER = objectNumber;
		this._TOTAL_REPS = totals;
		this._ROUNDS = _TOTAL_REPS / _CLIENT_NUMBER;
		all_keys = new TreeSet<Integer>();
		for (int i = 0; i < _OBJECT_NUMBER; i++)
			all_keys.add(i);
		// create all district keys
		all_keys_district = new TreeSet<DoubleKey>();
		for (int w = 0; w < _WAREHOUSE_NUMBER; w++) {
			for (int d = 0; d < _DISTRICT_NUMBER; d++) {
				all_keys_district.add(new DoubleKey(d, w));
				System.out.println(".");
			}
		}
		System.out.println("######"+all_keys_district.size());

		ser = TransactionIsolation.SERIALIZABLE;
		rc = TransactionIsolation.READ_COMMITTED;
		concurrency = TransactionConcurrency.PESSIMISTIC;
	}
}
