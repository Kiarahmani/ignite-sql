package com.mycompany.app;

public class Stat {
	String kind;
	long latency;

	public Stat(long latency, String kind) {
		this.latency = latency;
		this.kind = kind;
	}
}
