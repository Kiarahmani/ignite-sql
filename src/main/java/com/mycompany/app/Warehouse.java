package com.mycompany.app;

public class Warehouse {
	String w_name;
	String w_address;
	int w_tax;
	int w_ytd;
	public boolean isAlive;

	public Warehouse(String name, String address, int tax, int ytd, boolean isAlive) {
		this.w_name = name;
		this.w_address = address;
		this.w_ytd = ytd;
		this.w_tax = tax;
		this.isAlive = isAlive;
	}

	public String toString() {
		if (isAlive)
			return "WRHS[" + w_name + "," + w_address + "," + w_ytd + "," + w_tax + "]";
		else
			return "---";
	}
}
