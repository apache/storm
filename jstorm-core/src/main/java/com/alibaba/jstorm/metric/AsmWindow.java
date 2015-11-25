package com.alibaba.jstorm.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class AsmWindow {
    public static final Integer M1_WINDOW = 60;
    public static final Integer M10_WINDOW = 600;
    public static final Integer H2_WINDOW = 7200;
    public static final Integer D1_WINDOW = 86400;

    public static final String M1_WINDOW_STR = "0d0h1m0s";
    public static final String M10_WINDOW_STR = "0d0h10m0s";
    public static final String H2_WINDOW_STR = "0d2h0m0s";
    public static final String D1_WINDOW_STR = "1d0h0m0s";

    public static final Set<Integer> TIME_WINDOWS = new TreeSet<Integer>();
    private static final Map<Integer, String> WIN_TO_STR = new HashMap<Integer, String>();

    static {
        TIME_WINDOWS.add(M1_WINDOW);
        TIME_WINDOWS.add(M10_WINDOW);
        TIME_WINDOWS.add(H2_WINDOW);
        TIME_WINDOWS.add(D1_WINDOW);

        WIN_TO_STR.put(M1_WINDOW, M1_WINDOW_STR);
        WIN_TO_STR.put(M10_WINDOW, M10_WINDOW_STR);
        WIN_TO_STR.put(H2_WINDOW, H2_WINDOW_STR);
        WIN_TO_STR.put(D1_WINDOW, D1_WINDOW_STR);
    }

    public static String win2str(Integer win) {
        return WIN_TO_STR.get(win);
    }
}
