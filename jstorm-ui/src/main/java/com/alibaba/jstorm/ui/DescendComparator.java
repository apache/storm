package com.alibaba.jstorm.ui;

import java.util.Comparator;

public class DescendComparator implements Comparator {

	@Override
	public int compare(Object o1, Object o2) {

		if (o1 instanceof Double && o2 instanceof Double) {
			Double i1 = (Double) o1;
			Double i2 = (Double) o2;
			return -i1.compareTo(i2);
		} else if (o1 instanceof Integer && o2 instanceof Integer) {
			Integer i1 = (Integer) o1;
			Integer i2 = (Integer) o2;
			return -i1.compareTo(i2);
		} else {
			Double i1 = Double.valueOf(String.valueOf(o1));
			Double i2 = Double.valueOf(String.valueOf(o2));

			return -i1.compareTo(i2);
		}
	}
}
