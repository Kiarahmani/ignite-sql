package com.mycompany.app;

public class QuadKey implements Comparable<QuadKey> {
	public int k1;
	public int k2;
	public int k3;
	public int k4;

	public QuadKey(int k1, int k2, int k3, int k4) {
		this.k1 = k1;
		this.k2 = k2;
		this.k3 = k3;
		this.k4 = k4;
	}

	public int compareTo(QuadKey other) {
		if (k1 == other.k1) {
			if (k2 == other.k2) {
				if (k3 == other.k3)
					return (k4 - other.k4);
				else
					return (k3 - other.k3);

			} else {
				return (k2 - other.k2);
			}
		} else {
			return (k1 - other.k1);
		}

	}

	@Override
	public String toString() {
		return "$(" + k1 + "," + k2 + "," + k3 + "," + k4 + ")";
	}

	public boolean equals(QuadKey other) {
		return (k1 == other.k1 && k2 == other.k2 && k3 == other.k3 && k4 == other.k4);
	}

}
