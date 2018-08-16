package com.mycompany.app;

import java.util.Set;
import java.util.TreeSet;

import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

public class Constants {
	public int _CLIENT_NUMBER;
	public int _OBJECT_NUMBER;
	public int _TOTAL_REPS;
	public int _ROUNDS;
	Set<Integer> all_keys;

	TransactionIsolation ser;
	TransactionIsolation rc;
	TransactionConcurrency concurrency;

	public Constants(int clientNumber, int objectNumber, int totals) {
		this._CLIENT_NUMBER = clientNumber;
		this._OBJECT_NUMBER = objectNumber;
		this._TOTAL_REPS = totals;
		this._ROUNDS = _TOTAL_REPS / _CLIENT_NUMBER;
		all_keys = new TreeSet<Integer>();

		ser = TransactionIsolation.SERIALIZABLE;
		rc = TransactionIsolation.READ_COMMITTED;
		concurrency = TransactionConcurrency.PESSIMISTIC;
	}
}
