package com.alibaba.jstorm.common.stats;

import java.util.ArrayList;
import java.util.List;

public class StatBuckets {

	public static final Integer NUM_STAT_BUCKETS = 20;

	public static final Integer MINUTE_WINDOW = 600;
	public static final Integer HOUR_WINDOW = 10800;
	public static final Integer DAY_WINDOW = 86400;

	public static final String MINUTE_WINDOW_STR = "0d0h10m0s";
	public static final String HOUR_WINDOW_STR = "0d3h0m0s";
	public static final String DAY_WINDOW_STR = "1d0h0m0s";
	public static final String ALL_WINDOW_STR = "All-time";

	public static Integer[] STAT_BUCKETS = { MINUTE_WINDOW / NUM_STAT_BUCKETS,
			HOUR_WINDOW / NUM_STAT_BUCKETS, DAY_WINDOW / NUM_STAT_BUCKETS };

	private static final String[][] PRETTYSECDIVIDERS = {
			new String[] { "s", "60" }, new String[] { "m", "60" },
			new String[] { "h", "24" }, new String[] { "d", null } };

	/**
	 * Service b
	 * 
	 * @param key
	 * @return
	 */
	public static String parseTimeKey(Integer key) {
		if (key == 0) {
			return ALL_WINDOW_STR;
		} else {
			return String.valueOf(key);
		}
	}

	/**
	 * 
	 * Default is the latest result
	 * 
	 * @param showKey
	 * @return
	 */
	public static String getTimeKey(String showKey) {
		String window = null;
		if (showKey == null) {
			window = String.valueOf(MINUTE_WINDOW);
		} else if (showKey.equals(MINUTE_WINDOW_STR)) {
			window = String.valueOf(MINUTE_WINDOW);
		} else if (showKey.equals(HOUR_WINDOW_STR)) {
			window = String.valueOf(HOUR_WINDOW);
		} else if (showKey.equals(DAY_WINDOW_STR)) {
			window = String.valueOf(DAY_WINDOW);
		} else if (showKey.equals(ALL_WINDOW_STR)) {
			window = ALL_WINDOW_STR;
		} else {
			window = String.valueOf(MINUTE_WINDOW);
		}

		return window;
	}

	/**
	 * Default is the latest result
	 * 
	 * @param showStr
	 * @return
	 */
	public static String getShowTimeStr(String showStr) {
		if (showStr == null) {
			return MINUTE_WINDOW_STR;
		} else if (showStr.equals(MINUTE_WINDOW_STR)
				|| showStr.equals(HOUR_WINDOW_STR)
				|| showStr.equals(DAY_WINDOW_STR)
				|| showStr.equals(ALL_WINDOW_STR)) {
			return showStr;

		} else {
			return MINUTE_WINDOW_STR;
		}

	}

	/**
	 * seconds to string like 1d20h30m40s
	 * 
	 * @param secs
	 * @return
	 */
	public static String prettyUptimeStr(int secs) {
		int diversize = PRETTYSECDIVIDERS.length;

		List<String> tmp = new ArrayList<String>();
		int div = secs;
		for (int i = 0; i < diversize; i++) {
			if (PRETTYSECDIVIDERS[i][1] != null) {
				Integer d = Integer.parseInt(PRETTYSECDIVIDERS[i][1]);
				tmp.add(div % d + PRETTYSECDIVIDERS[i][0]);
				div = div / d;
			} else {
				tmp.add(div + PRETTYSECDIVIDERS[i][0]);
			}
		}

		String rtn = "";
		int tmpSzie = tmp.size();
		for (int j = tmpSzie - 1; j > -1; j--) {
			rtn += tmp.get(j);
		}
		return rtn;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
