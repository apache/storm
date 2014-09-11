package com.alipay.dw.jstorm.example.sequence.bean;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class TradeCustomer implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1294530416638900059L;
    protected final long      timestamp        = System.currentTimeMillis();
    protected Pair            trade;
    protected Pair            customer;
    protected String          buffer;
    
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
    
    public String getBuffer() {
		return buffer;
	}

	public void setBuffer(String buffer) {
		this.buffer = buffer;
	}

	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
    
}
