package com.mycompany.app;

public class TrippleKey implements Comparable<TrippleKey> {
	public int k1;
	public int k2;
	public int k3;

	public TrippleKey(int k1, int k2, int k3) {
		this.k1 = k1;
		this.k2 = k2;
		this.k3 = k3;
	}

	public int compareTo(TrippleKey other) {
		if (k1 == other.k1) {
			if (k2 == other.k2)
				return (k3 - other.k3);
			else
				return (k2 - other.k2);
		} else {
			return (k1 - other.k1);
		}

	}

	@Override
	public String toString() {
		return "$(" + k1 + "," + k2 + "," + k3 + ")";
	}

	public boolean equals(TrippleKey other) {
		return (k1 == other.k1 && k2 == other.k2 && k3 == other.k3);
	}

}
