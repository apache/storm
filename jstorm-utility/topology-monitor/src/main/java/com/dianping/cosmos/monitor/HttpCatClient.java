package com.dianping.cosmos.monitor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpCatClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientService.class);
    
    private HttpCatClient(){
    }

    private static HttpClientService httClientSerivce = new HttpClientService();
        
    private static List<String> CAT_SERVERS = new ArrayList<String>();
    //初始化访问的server的地址
    private static AtomicInteger CURRENT_SERVER_INDEX = new AtomicInteger(0);
    
    static{
        CAT_SERVERS.add("http://cat02.nh:8080/");
        CAT_SERVERS.add("http://cat03.nh:8080/");
        CAT_SERVERS.add("http://cat04.nh:8080/");
        CAT_SERVERS.add("http://cat05.nh:8080/");
        CAT_SERVERS.add("http://cat06.nh:8080/");
    }
    
    public static void sendMetric(String domain, String key, String op, String value){
        String server = getServer();
        try{
            StringBuilder request = new StringBuilder();
            request.append(server);
            request.append("cat/r/monitor?timestamp=");
            request.append(System.currentTimeMillis());
            request.append("&group=Storm&domain=");
            request.append(domain);
            request.append("&key=");
            request.append(key);
            request.append("&op=");
            request.append(op);
            request.append("&" + op +"=");
            request.append(value);
            httClientSerivce.get(request.toString());
        }
        catch(Exception e){
            CURRENT_SERVER_INDEX.getAndIncrement();
            LOGGER.error("send to cat " + server + " error.",  e);
        }
    }
    
    private static String getServer(){
        int index = CURRENT_SERVER_INDEX.get() % CAT_SERVERS.size();
        return CAT_SERVERS.get(index);
    }
}
