package com.dianping.cosmos.util;
import com.dianping.cat.Cat;

public class CatClient {
    
    private CatClient(){
    }
    
    private static Cat CAT = Cat.getInstance();
    
    static{
        Cat.initialize("cat02.nh","cat03.nh","cat04.nh","cat05.nh");
    }
    
    public static Cat getInstance(){
        return CAT;
    }
   
}
