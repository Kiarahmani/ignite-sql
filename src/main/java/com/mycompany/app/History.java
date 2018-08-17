package com.mycompany.app;

public class History {
	String h_info;
	boolean isAlive;

	public History(String info, boolean isAlive) {
		this.h_info = info;
		this.isAlive = isAlive;
	}

	@Override
	public String toString() {
		if (isAlive)
			return "ITEM[" + h_info + "]";
		else
			return "---";
	}
}