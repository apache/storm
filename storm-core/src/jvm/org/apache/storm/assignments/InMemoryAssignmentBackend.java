package org.apache.storm.assignments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An assignment backend which will keep all assignments and id-info in memory. Only used if no backend is specified internal.
 */
public class InMemoryAssignmentBackend implements ILocalAssignmentsBackend {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryAssignmentBackend.class);

    private Map<String, byte[]> idToAssignment;
    private Map<String, byte[]> idToName;
    private Map<String, byte[]> nameToId;

    private final Charset DEFAULT_CHARSET = Charset.defaultCharset();


    @Override
    public void prepare(Map conf, String localPath) {
        this.idToAssignment = new HashMap<>();
        this.idToName = new HashMap<>();
        this.nameToId = new HashMap<>();
    }

    @Override
    public void keepOrUpdateAssignment(String stormID, byte[] assignment) {
        this.idToAssignment.put(stormID, assignment);
    }

    @Override
    public byte[] getAssignment(String stormID) {
        return this.idToAssignment.get(stormID);
    }

    @Override
    public void removeAssignment(String stormID) {
        this.idToAssignment.remove(stormID);
    }

    @Override
    public List<String> assignments() {
        List<String> ids = new ArrayList<>();
        for (String key : this.idToAssignment.keySet()) {
            ids.add(key);
        }
        return ids;
    }

    @Override
    public Map<String, byte[]> assignmentsInfo() {
        return this.idToAssignment;
    }

    @Override
    public void syncRemoteAssignments(Map<String, byte[]> remote) {
        this.idToAssignment = remote;
    }

    @Override
    public void keepStormId(String stormName, String stormID) {
        this.nameToId.put(stormName, stormID.getBytes(DEFAULT_CHARSET));
        this.idToName.put(stormID, stormName.getBytes(DEFAULT_CHARSET));
    }

    @Override
    public String getStormId(String stormName) {
        byte[] id = this.nameToId.get(stormName);
        return id == null ? null : new String(id, DEFAULT_CHARSET);
    }

    @Override
    public void syncRemoteIDS(Map<String, String> remote) {
        Map<String, byte[]> tmpIdToName = new HashMap<>();
        Map<String, byte[]> tmpNameToID = new HashMap<>();
        for(Map.Entry<String, String> entry: remote.entrySet()) {
            tmpIdToName.put(entry.getKey(), entry.getValue().getBytes(DEFAULT_CHARSET));
            tmpNameToID.put(entry.getValue(), entry.getKey().getBytes(DEFAULT_CHARSET));
        }
        this.idToName = tmpIdToName;
        this.nameToId = tmpNameToID;
    }

    @Override
    public void deleteStormId(String stormName) {
        byte[] id = this.nameToId.remove(stormName);
        if (null != id) {
            this.idToName.remove(new String(id, DEFAULT_CHARSET));
        }
    }

    @Override
    public void clearStateForStorm(String stormID) {
        this.idToAssignment.remove(stormID);
        byte[] name = this.idToName.remove(stormID);
        if (null != name) {
            this.nameToId.remove(new String(name, DEFAULT_CHARSET));
        }
    }

    @Override
    public void dispose() {
        this.idToAssignment = null;
        this.nameToId = null;
        this.idToName = null;
    }
}
