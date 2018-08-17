package com.mycompany.app;

public class Item {
	String info;
	boolean isAlive;

	public Item(String info, boolean isAlive) {
		this.info = info;
		this.isAlive = isAlive;
	}

	public String toString() {
		if (isAlive)
			return "ORLN[" + info + "]";
		else
			return "---";
	}
}