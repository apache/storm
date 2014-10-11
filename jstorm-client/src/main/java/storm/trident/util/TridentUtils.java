package storm.trident.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift7.TBase;
import org.apache.thrift7.TDeserializer;
import org.apache.thrift7.TException;
import org.apache.thrift7.TSerializer;
import org.jgrapht.DirectedGraph;

import backtype.storm.generated.StreamInfo;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Fields;

public class TridentUtils {
    public static Fields fieldsUnion(Fields... fields) {
        Set<String> ret = new HashSet<String>();
        for(Fields f: fields) {
            if(f!=null) ret.addAll(f.toList());
        }
        return new Fields(new ArrayList<String>(ret));
    }
    
    public static Fields fieldsConcat(Fields... fields) {
        List<String> ret = new ArrayList<String>();
        for(Fields f: fields) {
            if(f!=null) ret.addAll(f.toList());
        }
        return new Fields(ret);
    }
    
    public static Fields fieldsSubtract(Fields all, Fields minus) {
        Set<String> removeSet = new HashSet<String>(minus.toList());
        List<String> toKeep = new ArrayList<String>();
        for(String s: all.toList()) {
            if(!removeSet.contains(s)) {
                toKeep.add(s);
            }
        }
        return new Fields(toKeep);
    }
    
    public static Fields getSingleOutputStreamFields(IComponent component) {
        OutputFieldsGetter getter = new OutputFieldsGetter();
        component.declareOutputFields(getter);
        Map<String, StreamInfo> declaration = getter.getFieldsDeclaration();
        if(declaration.size()!=1) {
            throw new RuntimeException("Trident only supports components that emit a single stream");
        }
        StreamInfo si = declaration.values().iterator().next();
        if(si.is_direct()) {
            throw new RuntimeException("Trident does not support direct streams");
        }
        return new Fields(si.get_output_fields());        
    }
    
    /**
     * Assumes edge contains an index
     */
    public static <T> List<T> getParents(DirectedGraph g, T n) {
        List<IndexedEdge> incoming = new ArrayList(g.incomingEdgesOf(n));
        Collections.sort(incoming);
        List<T> ret = new ArrayList();
        for(IndexedEdge e: incoming) {
            ret.add((T)e.source);
        }        
        return ret;
    }

    public static <T> List<T> getChildren(DirectedGraph g, T n) {
        List<IndexedEdge> outgoing = new ArrayList(g.outgoingEdgesOf(n));
        List<T> ret = new ArrayList();
        for(IndexedEdge e: outgoing) {
            ret.add((T)e.target);
        }        
        return ret;
    }
    
    
    public static <T> T getParent(DirectedGraph g, T n) {
        List<T> parents = getParents(g, n);
        if(parents.size()!=1) {
            throw new RuntimeException("Expected a single parent");
        }
        return parents.get(0);
    }
    
    private static ThreadLocal<TSerializer> threadSer = new ThreadLocal<TSerializer>();
    private static ThreadLocal<TDeserializer> threadDes = new ThreadLocal<TDeserializer>();
    
    public static byte[] thriftSerialize(TBase t) {
        try {
            TSerializer ser = threadSer.get();
            if (ser == null) {
                ser = new TSerializer();
                threadSer.set(ser);
            } 
            return ser.serialize(t);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T thriftDeserialize(Class c, byte[] b) {
        try {
            T ret = (T) c.newInstance();
            TDeserializer des = threadDes.get();
            if (des == null) {
                des = new TDeserializer();
                threadDes.set(des);
            }
            des.deserialize((TBase) ret, b);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }
}
