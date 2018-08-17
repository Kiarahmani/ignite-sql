package com.mycompany.app;

public class OrderLine {
	int ol_number;
	int ol_iid;
	String ol_delivdate;
	String ol_info;
	boolean isAlive;

	public OrderLine(int number, int iid, String deldate, String info, boolean isAlive) {
		this.ol_number = number;
		this.ol_iid = iid;
		this.ol_delivdate = deldate;
		this.ol_info = info;
		this.isAlive = isAlive;
	}

	public String toString() {
		if (isAlive)
			return "ORLN[" + ol_number + "," + ol_iid + "," + ol_delivdate + "," + ol_info + "]";
		else
			return "---";
	}
}
