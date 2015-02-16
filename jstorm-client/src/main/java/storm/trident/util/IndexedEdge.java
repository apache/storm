package storm.trident.util;

import java.io.Serializable;

public class IndexedEdge<T> implements Comparable, Serializable {
	public T source;
	public T target;
	public int index;

	public IndexedEdge(T source, T target, int index) {
		this.source = source;
		this.target = target;
		this.index = index;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + index;
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		result = prime * result + ((target == null) ? 0 : target.hashCode());
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
		IndexedEdge other = (IndexedEdge) obj;
		if (index != other.index)
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.equals(other.source))
			return false;
		if (target == null) {
			if (other.target != null)
				return false;
		} else if (!target.equals(other.target))
			return false;
		return true;
	}

	@Override
	public int compareTo(Object t) {
		IndexedEdge other = (IndexedEdge) t;
		return index - other.index;
	}
}
