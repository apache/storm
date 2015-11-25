package com.alibaba.jstorm.metric;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public enum MetaType {
    TASK(1, "T"), COMPONENT(2, "C"), STREAM(3, "S"), WORKER(4, "W"), TOPOLOGY(5, "P"), NETTY(6, "N"), NIMBUS(7, "M");

    private int t;
    private String v;

    MetaType(int t, String v) {
        this.t = t;
        this.v = v;
    }

    private static final Map<String, MetaType> valueMap = new HashMap<String, MetaType>();
    private static final Map<Integer, MetaType> typeMap = new HashMap<Integer, MetaType>();

    static {
        for (MetaType type : MetaType.values()) {
            typeMap.put(type.getT(), type);
            valueMap.put(type.getV(), type);
        }
    }

    public String getV() {
        return this.v;
    }

    public int getT() {
        return t;
    }

    public static MetaType parse(char ch) {
        return parse(ch + "");
    }

    public static MetaType parse(String v) {
        return valueMap.get(v);
    }

    public static MetaType parse(int t) {
        return typeMap.get(t);
    }
}
