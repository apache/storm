package com.alipay.dw.jstorm.example.sequence;


public class SequenceTopologyDef {
    
    public static final long   MAX_MESSAGE_COUNT   = 100000;
    
    public static final String SEQUENCE_SPOUT_NAME = "SequenceSpout";
    
    public static final String SPLIT_BOLT_NAME     = "Split";
    
    public static final String TRADE_BOLT_NAME     = "Trade";
    
    public static final String CUSTOMER_BOLT_NAME  = "Customer";
    
    public static final String MERGE_BOLT_NAME     = "Merge";
    
    public static final String TOTAL_BOLT_NAME     = "Total";
    
    public static final String TRADE_STREAM_ID     = "trade_stream";
    
    public static final String CUSTOMER_STREAM_ID  = "customer_stream";
    
}
