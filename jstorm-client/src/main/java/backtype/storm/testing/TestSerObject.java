package backtype.storm.testing;

import java.io.Serializable;

public class TestSerObject implements Serializable {
	public int f1;
	public int f2;

	public TestSerObject(int f1, int f2) {
		this.f1 = f1;
		this.f2 = f2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + f1;
		result = prime * result + f2;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TestSerObject other = (TestSerObject) obj;
		if (f1 != other.f1)
			return false;
		if (f2 != other.f2)
			return false;
		return true;
	}

	
}
