package com.mycompany.app;

public class Order {
	int o_carrier_id;
	String o_entry_date;
	public boolean isAlive;

	public Order(int oid, String oed, boolean isAlive) {
		this.o_carrier_id = oid;
		this.o_entry_date = oed;
		this.isAlive = isAlive;
	}

	public String toString() {
		if (isAlive)
			return "ORDR[" + o_carrier_id + "," + o_entry_date + "]";
		else
			return "---";
	}
}
