package com.mycompany.app;

public class Stats {
	long latency;
	String kind;

	public Stats(long latency, String kind) {
		this.kind = kind;
		this.latency = latency;
	}
}
