/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
