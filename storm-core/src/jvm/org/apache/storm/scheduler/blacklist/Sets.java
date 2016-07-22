package org.apache.storm.scheduler.blacklist;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by howard.li on 2016/7/8.
 */
public class Sets {

    public static <T> Set<T> union(Set<T> setA ,Set<T> setB){
        Set<T> result=new HashSet<T>(setA);
        result.addAll(setB);
        return result;
    }

    public static <T> Set<T> intersection(Set<T> setA ,Set<T> setB){
        Set<T> result=new HashSet<T>(setA);
        result.retainAll(setB);
        return result;
    }

    public static <T> Set<T> difference(Set<T> setA ,Set<T> setB){
        Set<T> result=new HashSet<T>(setA);
        result.removeAll(setB);
        return result;
    }

    public static <T> Set<T> symDifference(Set<T> setA ,Set<T> setB){
        Set<T> union=union(setA,setB);
        Set<T> intersection=intersection(setA,setB);
        return difference(union,intersection);
    }

    public static <T> boolean isSubset(Set<T> setA ,Set<T> setB){
        return setB.containsAll(setA);
    }


    public static <T> boolean isSuperset(Set<T> setA ,Set<T> setB){
        return setA.containsAll(setB);
    }

}
