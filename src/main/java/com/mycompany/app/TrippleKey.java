package com.mycompany.app;

public class TrippleKey {
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
				return (k3 > other.k3) ? 1 : 0;
			else
				return (k2 > other.k2) ? 1 : 0;
		} else {
			return (k1 > other.k1) ? 1 : 0;
		}

	}

	public boolean equals(TrippleKey other) {
		return (k1 == other.k1 && k2 == other.k2 && k3 == other.k3);
	}

}
