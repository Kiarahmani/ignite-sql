package com.mycompany.app;

public class Customer {
	String c_name;
	String c_address;
	int c_balance;
	int c_discount;
	int c_credit;
	int c_payment_count;
	int c_ytd;
	int c_deliverycnt;
	public boolean isAlive;

	public Customer(String name, String address, int balance, int discount, int credit, int paymentcnt, int ytd,
			int deliverycnt, boolean isAlive) {
		this.c_name = name;
		this.c_address = address;
		this.c_balance = balance;
		this.c_discount = discount;
		this.c_credit = credit;
		this.c_payment_count = paymentcnt;
		this.c_ytd = ytd;
		this.c_deliverycnt = deliverycnt;
		this.isAlive = isAlive;
	}

	public String toString() {
		if (isAlive)
			return "CSTR[" + c_name + "," + c_address + "," + c_balance + "," + c_discount + "," + c_credit + ","
					+ c_payment_count + "," + c_ytd + "," + c_deliverycnt + "]";
		else
			return "---";
	}
}
