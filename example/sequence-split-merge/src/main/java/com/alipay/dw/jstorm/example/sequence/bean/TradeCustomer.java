package com.alipay.dw.jstorm.example.sequence.bean;

import java.io.Serializable;

public class TradeCustomer implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1294530416638900059L;
    protected final long      timestamp        = System.currentTimeMillis();
    protected Pair            trade;
    protected Pair            customer;
    
    public Pair getTrade() {
        return trade;
    }
    
    public void setTrade(Pair trade) {
        this.trade = trade;
    }
    
    public Pair getCustomer() {
        return customer;
    }
    
    public void setCustomer(Pair customer) {
        this.customer = customer;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    
}
