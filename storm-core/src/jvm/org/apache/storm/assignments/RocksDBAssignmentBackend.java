/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.assignments;

import com.google.common.base.Preconditions;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.generated.Assignment;
import org.apache.storm.utils.Utils;
import org.rocksdb.*;
import org.slf4j.*;

import java.nio.charset.Charset;
import java.util.*;

/**
 * This class is used for nimbus/supervisor as an assignments store in local memory or disk.
 * <p/>
 * For nimbus, we put assignment to zk first every time we exec set-assignment! then to local,
 * The zookeeper assignments copy is used for HA recovery.
 * For newly elected leader, it will sync the whole assignments and id info from zookeeper then clean local corrupt state.
 * <p/>
 * For supervisors we just sync-assignment from remote.
 * <p>
 * To reduce jar conflict, user should add a dependency of rocks-db lib.
 */
public class RocksDBAssignmentBackend implements ILocalAssignmentsBackend {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RocksDBAssignmentBackend.class);


    /**
     * Column family for storing local assignments.
     */
    private final String COLUMN_ASSIGNMENTS = "column_assignments";

    /**
     * Column family for storing mapping storm-id -> storm-name
     */
    private final String COLUMN_STORM_ID_TO_NAME = "column_storm_id_to_name";

    /**
     * Column family for storing mapping storm-name -> storm->id
     */
    private final String COLUMN_STORM_NAME_TO_ID = "column_storm_name_to_id";

    private final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private String assignmentPath;

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB _db;
    private List<ColumnFamilyHandle> columnHandles;

    private Map<String, ColumnFamilyHandle> columnNameToHandles = new HashMap<>();

    private DBOptions dbOptions;

    @Override
    public void prepare(Map conf, String localPath) {
        Preconditions.checkArgument(localPath!=null, "base path for local assignments can not be null.");
        this.assignmentPath = localPath;

        checkAndCreateColumns();

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        appendColumnFamilyDescriptorInternal(columnFamilyDescriptors);

        columnHandles = new ArrayList<>(columnFamilyDescriptors.size());
        dbOptions = new DBOptions();
        dbOptions.setRecycleLogFileNum(3l);
        dbOptions.setKeepLogFileNum(10l);
        try {
            this._db = RocksDB.open(dbOptions, assignmentPath, columnFamilyDescriptors, columnHandles);
            bookColumnNameToHandle(columnFamilyDescriptors, columnHandles);
        } catch (RocksDBException e) {
            dispose();
            LOG.error("Can not initialize rocks_db from path {}", assignmentPath);
            throw new RuntimeException(e);
        }
    }

    private void appendColumnFamilyDescriptorInternal(List<ColumnFamilyDescriptor> columnFamilyDescriptors) {
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(
                        RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(
                        COLUMN_ASSIGNMENTS.getBytes(DEFAULT_CHARSET), new ColumnFamilyOptions()));
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(
                        COLUMN_STORM_ID_TO_NAME.getBytes(DEFAULT_CHARSET), new ColumnFamilyOptions()));
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(
                        COLUMN_STORM_NAME_TO_ID.getBytes(DEFAULT_CHARSET), new ColumnFamilyOptions()));
    }

    private void bookColumnNameToHandle(List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> handles) {
        Preconditions.checkState(descriptors.size() == handles.size(), "RocksDB descriptors and handlers size doesn't match!");
        for (int i = 0; i < descriptors.size(); i++) {
            columnNameToHandles.put(new String(descriptors.get(i).columnFamilyName(), DEFAULT_CHARSET), handles.get(i));
        }
    }

    @Override
    public void keepOrUpdateAssignment(String stormID, Assignment assignment) {
        keepRecordInternal(COLUMN_ASSIGNMENTS, stormID, Utils.serialize(assignment));
    }

    @Override
    public Assignment getAssignment(String stormID) {
        byte[] ser = getRecordInternal(COLUMN_ASSIGNMENTS, stormID);
        return ClusterUtils.maybeDeserialize(ser, Assignment.class);
    }

    @Override
    public void removeAssignment(String stormID) {
        deleteRecordInternal(COLUMN_ASSIGNMENTS, stormID);
    }

    /**
     * Get all the assignments ids.
     *
     * @return
     */
    @Override
    public List<String> assignments() {
        ColumnFamilyHandle handle = getColumnHandleByName(COLUMN_ASSIGNMENTS);
        List<String> ids = new ArrayList<>();
        try (final RocksIterator iterator = this._db.newIterator(handle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                ids.add(new String(iterator.key(), DEFAULT_CHARSET));
            }
        }
        return ids;
    }

    /**
     * Get all the assignments from backend
     *
     * @return
     */
    @Override
    public Map<String, Assignment> assignmentsInfo() {
        ColumnFamilyHandle handle = getColumnHandleByName(COLUMN_ASSIGNMENTS);
        Map<String, Assignment> ret = new HashMap<>();
        try (final RocksIterator iterator = this._db.newIterator(handle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                ret.put(new String(iterator.key(), DEFAULT_CHARSET), ClusterUtils.maybeDeserialize(iterator.value(), Assignment.class));
            }
        }
        return ret;
    }

    @Override
    public void syncRemoteAssignments(Map<String, byte[]> remote) {
        try (final WriteBatch wb = new WriteBatch()) {
            for (Map.Entry<String, byte[]> assignment : remote.entrySet()) {
                wb.put(getColumnHandleByName(COLUMN_ASSIGNMENTS), assignment.getKey().getBytes(DEFAULT_CHARSET), assignment.getValue());
            }
            this._db.write(new WriteOptions(), wb);
        } catch (RocksDBException e) {
            LOG.error("Exception when sync assignments from remote.");
            dispose();
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        clearExpiredAssignments(remote.keySet());
    }

    @Override
    public void keepStormId(String stormName, String stormID) {
        try (final WriteBatch wb = new WriteBatch()) {
            wb.put(getColumnHandleByName(COLUMN_STORM_NAME_TO_ID), stormName.getBytes(DEFAULT_CHARSET), stormID.getBytes(DEFAULT_CHARSET));
            wb.put(getColumnHandleByName(COLUMN_STORM_ID_TO_NAME), stormID.getBytes(DEFAULT_CHARSET), stormName.getBytes(DEFAULT_CHARSET));
            this._db.write(new WriteOptions(), wb);
        } catch (RocksDBException ex) {
            dispose();
            LOG.error("Exception when keep storm-id: {} storm-name: {}", stormID, stormName);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public String getStormId(String stormName) {
        byte[] stormId = getRecordInternal(COLUMN_STORM_NAME_TO_ID, stormName);
        return stormId == null ? null : new String(stormId, DEFAULT_CHARSET);
    }

    @Override
    public void syncRemoteIDS(Map<String, String> remote) {
        try (final WriteBatch wb = new WriteBatch()) {
            for (Map.Entry<String, String> idToName : remote.entrySet()) {
                wb.put(getColumnHandleByName(COLUMN_STORM_ID_TO_NAME), idToName.getKey().getBytes(DEFAULT_CHARSET),
                        idToName.getValue().getBytes(DEFAULT_CHARSET));
                wb.put(getColumnHandleByName(COLUMN_STORM_NAME_TO_ID), idToName.getValue().getBytes(DEFAULT_CHARSET),
                        idToName.getKey().getBytes(DEFAULT_CHARSET));
            }
            this._db.write(new WriteOptions(), wb);
        } catch (RocksDBException e) {
            LOG.error("Exception when sync assignments from remote.");
            dispose();
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        clearExpiredIDS(remote.keySet());
    }

    @Override
    public void deleteStormId(String stormName) {
        deleteRecordInternal(COLUMN_STORM_NAME_TO_ID, stormName);
    }

    @Override
    public void clearStateForStorm(String stormID) {
        try (final WriteBatch wb = new WriteBatch()) {
            wb.remove(getColumnHandleByName(COLUMN_ASSIGNMENTS), stormID.getBytes(DEFAULT_CHARSET));
            byte[] stormName = getRecordInternal(COLUMN_STORM_ID_TO_NAME, stormID);
            if (stormName != null) {
                wb.remove(getColumnHandleByName(COLUMN_STORM_NAME_TO_ID), stormName);
            }
            wb.remove(getColumnHandleByName(COLUMN_STORM_ID_TO_NAME), stormID.getBytes(DEFAULT_CHARSET));
            this._db.write(new WriteOptions(), wb);
        } catch (RocksDBException ex) {
            dispose();
            LOG.error("Exception when clean assignment state for storm: {}", stormID);
            throw new RuntimeException(ex);
        }

    }

    private void deleteRecordInternal(String column, String k) {
        try {
            this._db.delete(getColumnHandleByName(column), k.getBytes(DEFAULT_CHARSET));
        } catch (RocksDBException e) {
            dispose();
            LOG.error("Exception to get for key: {} from rocks db path: {}", k, assignmentPath);
            throw new RuntimeException(e);
        }
    }

    private byte[] getRecordInternal(String column, String k) {
        try {
            return this._db.get(getColumnHandleByName(column), k.getBytes(DEFAULT_CHARSET));
        } catch (RocksDBException e) {
            dispose();
            LOG.error("Exception to get for key: {} from rocks db path: {}", k, assignmentPath);
            throw new RuntimeException(e);
        }
    }

    private ColumnFamilyHandle getColumnHandleByName(String column) {
        ColumnFamilyHandle handle = this.columnNameToHandles.get(column);
        Preconditions.checkState(handle != null, "Can not find column handle for name: {}", column);
        return handle;
    }

    private void keepRecordInternal(String column, String k, String v) {
        keepRecordInternal(column, k, v.getBytes(DEFAULT_CHARSET));
    }


    private void keepRecordInternal(String column, String k, byte[] v) {
        byte[] tmpK = k.getBytes(DEFAULT_CHARSET);
        byte[] tmpV = v;
        ColumnFamilyHandle handle = getColumnHandleByName(column);
        int tryTimes = 0;
        while (tryTimes < 3) {
            try {
                this._db.put(handle, tmpK, tmpV);
                break;
            } catch (RocksDBException e) {
                tryTimes++;
                if (tryTimes == 2) {
                    LOG.error("Exception to put record {}th time with key: {}, val: {}, columnName: {}", tryTimes, k, v, column);
                    e.printStackTrace();
                    dispose();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * We should invoke this when any exception happens to release resource.
     */
    @Override
    public void dispose() {
        for (ColumnFamilyHandle cfh : this.columnHandles) {
            try {
                cfh.close();
            } catch (Exception var1) {
                LOG.info("Exception while closing ColumnFamilyHandle object.", var1);
            }
        }

        if (_db != null) {
            try {
                _db.close();
            } catch (Exception ex) {
                LOG.info("Exception while closing RocksDB object.", ex);
            }
        }
        _db = null;
        if (null != dbOptions) {
            closeQuietly(dbOptions);
        }
    }

    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Throwable ignored) {
        }
    }

    private void clearExpiredAssignments(Set<String> keys) {
        ColumnFamilyHandle handleAssignments = getColumnHandleByName(COLUMN_ASSIGNMENTS);
        try (final RocksIterator iterator = this._db.newIterator(handleAssignments);
             final WriteBatch wb1 = new WriteBatch()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String localID = new String(iterator.key(), DEFAULT_CHARSET);
                if (!keys.contains(localID)) {
                    wb1.remove(handleAssignments, iterator.key());
                }
            }
            this._db.write(new WriteOptions(), wb1);
        } catch (RocksDBException e) {
            LOG.error("Exception to remove expired keys for assignment.");
            this.dispose();
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void clearExpiredIDS(Set<String> keys) {

        ColumnFamilyHandle handleIDName = getColumnHandleByName(COLUMN_STORM_ID_TO_NAME);
        ColumnFamilyHandle handleNameID = getColumnHandleByName(COLUMN_STORM_NAME_TO_ID);
        try (final RocksIterator iterator = this._db.newIterator(handleIDName);
             final RocksIterator iterator1 = this._db.newIterator(handleNameID);
             final WriteBatch wb = new WriteBatch()) {
            //remove storm-id -> storm-name/ storm-name -> storm-id mapping
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String localID = new String(iterator.key(), DEFAULT_CHARSET);
                if (!keys.contains(localID)) {
                    wb.remove(handleIDName, iterator.key());

                }
            }
            for (iterator1.seekToFirst(); iterator1.isValid(); iterator1.next()) {
                String localID = new String(iterator1.value(), DEFAULT_CHARSET);
                if (!keys.contains(localID)) {
                    wb.remove(handleNameID, iterator1.key());

                }
            }

            this._db.write(new WriteOptions(), wb);
        } catch (RocksDBException e) {
            LOG.error("Exception to remove expired keys for local id info.");
            this.dispose();
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void checkAndCreateColumns() {
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try {
                List<byte[]> columns = RocksDB.listColumnFamilies(options, assignmentPath);
                LOG.info("Got {} columns from path {}", columns.size(), assignmentPath);
                if (columns == null || columns.size() < 3) {
                    //create columns
                    try (final RocksDB db = RocksDB.open(options, assignmentPath)){

                        assert (db != null);

                        // create column assignments
                        try (final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
                                new ColumnFamilyDescriptor(COLUMN_ASSIGNMENTS.getBytes(),
                                        new ColumnFamilyOptions()))) {
                            assert (columnFamilyHandle != null);
                        }
                        // create column id to name
                        try (final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
                                new ColumnFamilyDescriptor(COLUMN_STORM_ID_TO_NAME.getBytes(),
                                        new ColumnFamilyOptions()))) {
                            assert (columnFamilyHandle != null);
                        }

                        // create column name to id
                        try (final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
                                new ColumnFamilyDescriptor(COLUMN_STORM_NAME_TO_ID.getBytes(),
                                        new ColumnFamilyOptions()))) {
                            assert (columnFamilyHandle != null);
                        }
                    } catch (RocksDBException e) {
                        LOG.debug("Exception to create columns: [{} | {} | {}]", COLUMN_ASSIGNMENTS, COLUMN_STORM_ID_TO_NAME, COLUMN_STORM_NAME_TO_ID);
                        throw new RuntimeException(e);
                    }
                }
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }
}
