package com.alibaba.jstorm.utils;

import java.util.ArrayList;

/**
 * Shuffle the Range, This class is used in shuffle grouping, it is better than
 * random, which can't make sure balance.
 * 
 * @author yannian
 * 
 */
public class RandomRange {
	private ArrayList<Integer> rr;
	private Integer amt;

	public RandomRange(int amt) {
		this.amt = amt;
		this.rr = rotating_random_range(amt);
	}

	public Integer nextInt() {
		return this.acquire_random_range_id();
	}

	private ArrayList<Integer> rotating_random_range(int amt) {

		ArrayList<Integer> range = new ArrayList<Integer>();
		for (int i = 0; i < amt; i++) {
			range.add(i);
		}

		ArrayList<Integer> rtn = new ArrayList<Integer>();
		for (int i = 0; i < amt; i++) {
			int index = (int) (Math.random() * range.size());
			rtn.add(range.remove(index));
		}

		return rtn;
	}

	private synchronized int acquire_random_range_id() {
		int ret = this.rr.remove(0);
		if (this.rr.size() == 0) {
			this.rr.addAll(rotating_random_range(this.amt));
		}
		return ret;
	}

	public static void main(String[] args) {
		RandomRange test = new RandomRange(10);

		for (int i = 0; i < 10; i++) {
			System.out.println(test.acquire_random_range_id());
		}
	}

}
