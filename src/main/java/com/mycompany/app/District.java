package com.mycompany.app;

public class District {
	String d_name;
	String d_address;
	int d_ytd;
	int d_nextoid;
	public boolean isAlive;

	public District(String name, String address, int ytd, int nextoid, boolean isAlive) {
		this.d_name = name;
		this.d_address = address;
		this.d_ytd = ytd;
		this.d_nextoid = nextoid;
		this.isAlive = isAlive;
	}

	public String toString() {
		if (isAlive)
			return "DIST[" + d_name + "," + d_address + "," + d_ytd + "," + d_nextoid + "]";
		else
			return "---";
	}

}
