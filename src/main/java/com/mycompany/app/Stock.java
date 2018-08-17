package com.mycompany.app;

public class Stock {
	int s_ytd;
	int s_quant;
	int s_ordercnt;
	String s_info;
	boolean isAlive;

	public Stock(int ytd, int quant, int count, String info, boolean isAlive) {
		this.s_ytd = ytd;
		this.s_quant = quant;
		this.s_ordercnt = count;
		this.s_info = info;
		this.isAlive = isAlive;
	}

	@Override
	public String toString() {
		if (isAlive)
			return "STCK" + s_ytd + "," + s_quant + "," + s_ordercnt + "," + s_info + "]";
		else
			return "---";
	}
}
