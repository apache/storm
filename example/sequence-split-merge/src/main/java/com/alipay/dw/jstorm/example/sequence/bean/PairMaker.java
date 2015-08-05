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

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


public class PairMaker {
    private static final String[] tradeType = new String[] { "trade_one",
        "trade_two", "trade_three", "trade_four", "trade_five",
        "trade_six", "trade_seven", "trade_eight", "trade_nine",
        "trade_ten" };

    private static final String[] customerType = new String[] { "customer_a",
        "customer_b", "customer_c", "customer_d", "customer_e",
        "customer_f" };
    
    private static Random rand = new Random();
    
    
    

    public static Pair makeTradeInstance() {
        Pair pair = new Pair();
        
        String key = tradeType[rand.nextInt(tradeType.length)];
        Long   value = Long.valueOf(((rand.nextInt(1000000) - 400000) / 1000));

        
        pair.setKey(key);
        pair.setValue(value);
        
        return pair;
    }
    
    public static Pair makeCustomerInstance() {
        Pair pair = new Pair();
        
        String key = customerType[rand.nextInt(customerType.length)];
        Long   value = Long.valueOf(((rand.nextInt(1000000) - 200000) / 1000));

        
        pair.setKey(key);
        pair.setValue(value);
        
        return pair;
    }
    
}
