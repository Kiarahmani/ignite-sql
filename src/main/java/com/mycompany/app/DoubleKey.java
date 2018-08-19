package com.mycompany.app;

public class DoubleKey implements Comparable<DoubleKey> {
	public int k1;
	public int k2;

	public DoubleKey(int k1, int k2) {
		this.k1 = k1;
		this.k2 = k2;
	}

	public int compareTo(DoubleKey other) {
		if (k1 == other.k1)
			return (k2 >= other.k2) ? 1 : 0;
		else
			return (k1 > other.k1) ? 1 : 0;

	}

	public String toString() {
		return "$(" + k1 + "," + k2 + ")";
	}

	public boolean equals(DoubleKey other) {
		return (k1 == other.k1 && k2 == other.k2);
	}

}
