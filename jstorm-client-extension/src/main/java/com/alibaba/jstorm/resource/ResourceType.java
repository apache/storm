package com.alibaba.jstorm.resource;


public enum ResourceType {
    UNKNOWN,
    CPU,
    MEM,
    NET,
    DISK;
   
    
    public static ResourceType toResourceType(int type) {
        if (type == CPU.ordinal()) {
            return CPU;
        }else if (type == MEM.ordinal()) {
            return MEM;
        }else if (type == NET.ordinal()) {
            return NET;
        }else if (type == DISK.ordinal()) {
            return DISK;
        }else  {
            return UNKNOWN;
        }
    }
}
