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
