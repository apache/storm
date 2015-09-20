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
/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package backtype.storm.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-9-21")
public class SupervisorSummary implements org.apache.thrift.TBase<SupervisorSummary, SupervisorSummary._Fields>, java.io.Serializable, Cloneable, Comparable<SupervisorSummary> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SupervisorSummary");

  private static final org.apache.thrift.protocol.TField HOST_FIELD_DESC = new org.apache.thrift.protocol.TField("host", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField UPTIME_SECS_FIELD_DESC = new org.apache.thrift.protocol.TField("uptime_secs", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField NUM_WORKERS_FIELD_DESC = new org.apache.thrift.protocol.TField("num_workers", org.apache.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.thrift.protocol.TField NUM_USED_WORKERS_FIELD_DESC = new org.apache.thrift.protocol.TField("num_used_workers", org.apache.thrift.protocol.TType.I32, (short)4);
  private static final org.apache.thrift.protocol.TField SUPERVISOR_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("supervisor_id", org.apache.thrift.protocol.TType.STRING, (short)5);
  private static final org.apache.thrift.protocol.TField VERSION_FIELD_DESC = new org.apache.thrift.protocol.TField("version", org.apache.thrift.protocol.TType.STRING, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SupervisorSummaryStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SupervisorSummaryTupleSchemeFactory());
  }

  private String host; // required
  private int uptime_secs; // required
  private int num_workers; // required
  private int num_used_workers; // required
  private String supervisor_id; // required
  private String version; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    HOST((short)1, "host"),
    UPTIME_SECS((short)2, "uptime_secs"),
    NUM_WORKERS((short)3, "num_workers"),
    NUM_USED_WORKERS((short)4, "num_used_workers"),
    SUPERVISOR_ID((short)5, "supervisor_id"),
    VERSION((short)6, "version");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // HOST
          return HOST;
        case 2: // UPTIME_SECS
          return UPTIME_SECS;
        case 3: // NUM_WORKERS
          return NUM_WORKERS;
        case 4: // NUM_USED_WORKERS
          return NUM_USED_WORKERS;
        case 5: // SUPERVISOR_ID
          return SUPERVISOR_ID;
        case 6: // VERSION
          return VERSION;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __UPTIME_SECS_ISSET_ID = 0;
  private static final int __NUM_WORKERS_ISSET_ID = 1;
  private static final int __NUM_USED_WORKERS_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.VERSION};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.HOST, new org.apache.thrift.meta_data.FieldMetaData("host", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.UPTIME_SECS, new org.apache.thrift.meta_data.FieldMetaData("uptime_secs", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.NUM_WORKERS, new org.apache.thrift.meta_data.FieldMetaData("num_workers", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.NUM_USED_WORKERS, new org.apache.thrift.meta_data.FieldMetaData("num_used_workers", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.SUPERVISOR_ID, new org.apache.thrift.meta_data.FieldMetaData("supervisor_id", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.VERSION, new org.apache.thrift.meta_data.FieldMetaData("version", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SupervisorSummary.class, metaDataMap);
  }

  public SupervisorSummary() {
    this.version = "VERSION_NOT_PROVIDED";

  }

  public SupervisorSummary(
    String host,
    int uptime_secs,
    int num_workers,
    int num_used_workers,
    String supervisor_id)
  {
    this();
    this.host = host;
    this.uptime_secs = uptime_secs;
    set_uptime_secs_isSet(true);
    this.num_workers = num_workers;
    set_num_workers_isSet(true);
    this.num_used_workers = num_used_workers;
    set_num_used_workers_isSet(true);
    this.supervisor_id = supervisor_id;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SupervisorSummary(SupervisorSummary other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.is_set_host()) {
      this.host = other.host;
    }
    this.uptime_secs = other.uptime_secs;
    this.num_workers = other.num_workers;
    this.num_used_workers = other.num_used_workers;
    if (other.is_set_supervisor_id()) {
      this.supervisor_id = other.supervisor_id;
    }
    if (other.is_set_version()) {
      this.version = other.version;
    }
  }

  public SupervisorSummary deepCopy() {
    return new SupervisorSummary(this);
  }

  @Override
  public void clear() {
    this.host = null;
    set_uptime_secs_isSet(false);
    this.uptime_secs = 0;
    set_num_workers_isSet(false);
    this.num_workers = 0;
    set_num_used_workers_isSet(false);
    this.num_used_workers = 0;
    this.supervisor_id = null;
    this.version = "VERSION_NOT_PROVIDED";

  }

  public String get_host() {
    return this.host;
  }

  public void set_host(String host) {
    this.host = host;
  }

  public void unset_host() {
    this.host = null;
  }

  /** Returns true if field host is set (has been assigned a value) and false otherwise */
  public boolean is_set_host() {
    return this.host != null;
  }

  public void set_host_isSet(boolean value) {
    if (!value) {
      this.host = null;
    }
  }

  public int get_uptime_secs() {
    return this.uptime_secs;
  }

  public void set_uptime_secs(int uptime_secs) {
    this.uptime_secs = uptime_secs;
    set_uptime_secs_isSet(true);
  }

  public void unset_uptime_secs() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __UPTIME_SECS_ISSET_ID);
  }

  /** Returns true if field uptime_secs is set (has been assigned a value) and false otherwise */
  public boolean is_set_uptime_secs() {
    return EncodingUtils.testBit(__isset_bitfield, __UPTIME_SECS_ISSET_ID);
  }

  public void set_uptime_secs_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __UPTIME_SECS_ISSET_ID, value);
  }

  public int get_num_workers() {
    return this.num_workers;
  }

  public void set_num_workers(int num_workers) {
    this.num_workers = num_workers;
    set_num_workers_isSet(true);
  }

  public void unset_num_workers() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUM_WORKERS_ISSET_ID);
  }

  /** Returns true if field num_workers is set (has been assigned a value) and false otherwise */
  public boolean is_set_num_workers() {
    return EncodingUtils.testBit(__isset_bitfield, __NUM_WORKERS_ISSET_ID);
  }

  public void set_num_workers_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUM_WORKERS_ISSET_ID, value);
  }

  public int get_num_used_workers() {
    return this.num_used_workers;
  }

  public void set_num_used_workers(int num_used_workers) {
    this.num_used_workers = num_used_workers;
    set_num_used_workers_isSet(true);
  }

  public void unset_num_used_workers() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUM_USED_WORKERS_ISSET_ID);
  }

  /** Returns true if field num_used_workers is set (has been assigned a value) and false otherwise */
  public boolean is_set_num_used_workers() {
    return EncodingUtils.testBit(__isset_bitfield, __NUM_USED_WORKERS_ISSET_ID);
  }

  public void set_num_used_workers_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUM_USED_WORKERS_ISSET_ID, value);
  }

  public String get_supervisor_id() {
    return this.supervisor_id;
  }

  public void set_supervisor_id(String supervisor_id) {
    this.supervisor_id = supervisor_id;
  }

  public void unset_supervisor_id() {
    this.supervisor_id = null;
  }

  /** Returns true if field supervisor_id is set (has been assigned a value) and false otherwise */
  public boolean is_set_supervisor_id() {
    return this.supervisor_id != null;
  }

  public void set_supervisor_id_isSet(boolean value) {
    if (!value) {
      this.supervisor_id = null;
    }
  }

  public String get_version() {
    return this.version;
  }

  public void set_version(String version) {
    this.version = version;
  }

  public void unset_version() {
    this.version = null;
  }

  /** Returns true if field version is set (has been assigned a value) and false otherwise */
  public boolean is_set_version() {
    return this.version != null;
  }

  public void set_version_isSet(boolean value) {
    if (!value) {
      this.version = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case HOST:
      if (value == null) {
        unset_host();
      } else {
        set_host((String)value);
      }
      break;

    case UPTIME_SECS:
      if (value == null) {
        unset_uptime_secs();
      } else {
        set_uptime_secs((Integer)value);
      }
      break;

    case NUM_WORKERS:
      if (value == null) {
        unset_num_workers();
      } else {
        set_num_workers((Integer)value);
      }
      break;

    case NUM_USED_WORKERS:
      if (value == null) {
        unset_num_used_workers();
      } else {
        set_num_used_workers((Integer)value);
      }
      break;

    case SUPERVISOR_ID:
      if (value == null) {
        unset_supervisor_id();
      } else {
        set_supervisor_id((String)value);
      }
      break;

    case VERSION:
      if (value == null) {
        unset_version();
      } else {
        set_version((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case HOST:
      return get_host();

    case UPTIME_SECS:
      return Integer.valueOf(get_uptime_secs());

    case NUM_WORKERS:
      return Integer.valueOf(get_num_workers());

    case NUM_USED_WORKERS:
      return Integer.valueOf(get_num_used_workers());

    case SUPERVISOR_ID:
      return get_supervisor_id();

    case VERSION:
      return get_version();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case HOST:
      return is_set_host();
    case UPTIME_SECS:
      return is_set_uptime_secs();
    case NUM_WORKERS:
      return is_set_num_workers();
    case NUM_USED_WORKERS:
      return is_set_num_used_workers();
    case SUPERVISOR_ID:
      return is_set_supervisor_id();
    case VERSION:
      return is_set_version();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SupervisorSummary)
      return this.equals((SupervisorSummary)that);
    return false;
  }

  public boolean equals(SupervisorSummary that) {
    if (that == null)
      return false;

    boolean this_present_host = true && this.is_set_host();
    boolean that_present_host = true && that.is_set_host();
    if (this_present_host || that_present_host) {
      if (!(this_present_host && that_present_host))
        return false;
      if (!this.host.equals(that.host))
        return false;
    }

    boolean this_present_uptime_secs = true;
    boolean that_present_uptime_secs = true;
    if (this_present_uptime_secs || that_present_uptime_secs) {
      if (!(this_present_uptime_secs && that_present_uptime_secs))
        return false;
      if (this.uptime_secs != that.uptime_secs)
        return false;
    }

    boolean this_present_num_workers = true;
    boolean that_present_num_workers = true;
    if (this_present_num_workers || that_present_num_workers) {
      if (!(this_present_num_workers && that_present_num_workers))
        return false;
      if (this.num_workers != that.num_workers)
        return false;
    }

    boolean this_present_num_used_workers = true;
    boolean that_present_num_used_workers = true;
    if (this_present_num_used_workers || that_present_num_used_workers) {
      if (!(this_present_num_used_workers && that_present_num_used_workers))
        return false;
      if (this.num_used_workers != that.num_used_workers)
        return false;
    }

    boolean this_present_supervisor_id = true && this.is_set_supervisor_id();
    boolean that_present_supervisor_id = true && that.is_set_supervisor_id();
    if (this_present_supervisor_id || that_present_supervisor_id) {
      if (!(this_present_supervisor_id && that_present_supervisor_id))
        return false;
      if (!this.supervisor_id.equals(that.supervisor_id))
        return false;
    }

    boolean this_present_version = true && this.is_set_version();
    boolean that_present_version = true && that.is_set_version();
    if (this_present_version || that_present_version) {
      if (!(this_present_version && that_present_version))
        return false;
      if (!this.version.equals(that.version))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_host = true && (is_set_host());
    list.add(present_host);
    if (present_host)
      list.add(host);

    boolean present_uptime_secs = true;
    list.add(present_uptime_secs);
    if (present_uptime_secs)
      list.add(uptime_secs);

    boolean present_num_workers = true;
    list.add(present_num_workers);
    if (present_num_workers)
      list.add(num_workers);

    boolean present_num_used_workers = true;
    list.add(present_num_used_workers);
    if (present_num_used_workers)
      list.add(num_used_workers);

    boolean present_supervisor_id = true && (is_set_supervisor_id());
    list.add(present_supervisor_id);
    if (present_supervisor_id)
      list.add(supervisor_id);

    boolean present_version = true && (is_set_version());
    list.add(present_version);
    if (present_version)
      list.add(version);

    return list.hashCode();
  }

  @Override
  public int compareTo(SupervisorSummary other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_host()).compareTo(other.is_set_host());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_host()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.host, other.host);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_uptime_secs()).compareTo(other.is_set_uptime_secs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_uptime_secs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.uptime_secs, other.uptime_secs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_num_workers()).compareTo(other.is_set_num_workers());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_num_workers()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.num_workers, other.num_workers);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_num_used_workers()).compareTo(other.is_set_num_used_workers());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_num_used_workers()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.num_used_workers, other.num_used_workers);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_supervisor_id()).compareTo(other.is_set_supervisor_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_supervisor_id()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.supervisor_id, other.supervisor_id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_version()).compareTo(other.is_set_version());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_version()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.version, other.version);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SupervisorSummary(");
    boolean first = true;

    sb.append("host:");
    if (this.host == null) {
      sb.append("null");
    } else {
      sb.append(this.host);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("uptime_secs:");
    sb.append(this.uptime_secs);
    first = false;
    if (!first) sb.append(", ");
    sb.append("num_workers:");
    sb.append(this.num_workers);
    first = false;
    if (!first) sb.append(", ");
    sb.append("num_used_workers:");
    sb.append(this.num_used_workers);
    first = false;
    if (!first) sb.append(", ");
    sb.append("supervisor_id:");
    if (this.supervisor_id == null) {
      sb.append("null");
    } else {
      sb.append(this.supervisor_id);
    }
    first = false;
    if (is_set_version()) {
      if (!first) sb.append(", ");
      sb.append("version:");
      if (this.version == null) {
        sb.append("null");
      } else {
        sb.append(this.version);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_host()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'host' is unset! Struct:" + toString());
    }

    if (!is_set_uptime_secs()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'uptime_secs' is unset! Struct:" + toString());
    }

    if (!is_set_num_workers()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'num_workers' is unset! Struct:" + toString());
    }

    if (!is_set_num_used_workers()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'num_used_workers' is unset! Struct:" + toString());
    }

    if (!is_set_supervisor_id()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'supervisor_id' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class SupervisorSummaryStandardSchemeFactory implements SchemeFactory {
    public SupervisorSummaryStandardScheme getScheme() {
      return new SupervisorSummaryStandardScheme();
    }
  }

  private static class SupervisorSummaryStandardScheme extends StandardScheme<SupervisorSummary> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SupervisorSummary struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // HOST
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.host = iprot.readString();
              struct.set_host_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // UPTIME_SECS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.uptime_secs = iprot.readI32();
              struct.set_uptime_secs_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // NUM_WORKERS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.num_workers = iprot.readI32();
              struct.set_num_workers_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // NUM_USED_WORKERS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.num_used_workers = iprot.readI32();
              struct.set_num_used_workers_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // SUPERVISOR_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.supervisor_id = iprot.readString();
              struct.set_supervisor_id_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // VERSION
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.version = iprot.readString();
              struct.set_version_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, SupervisorSummary struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.host != null) {
        oprot.writeFieldBegin(HOST_FIELD_DESC);
        oprot.writeString(struct.host);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(UPTIME_SECS_FIELD_DESC);
      oprot.writeI32(struct.uptime_secs);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(NUM_WORKERS_FIELD_DESC);
      oprot.writeI32(struct.num_workers);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(NUM_USED_WORKERS_FIELD_DESC);
      oprot.writeI32(struct.num_used_workers);
      oprot.writeFieldEnd();
      if (struct.supervisor_id != null) {
        oprot.writeFieldBegin(SUPERVISOR_ID_FIELD_DESC);
        oprot.writeString(struct.supervisor_id);
        oprot.writeFieldEnd();
      }
      if (struct.version != null) {
        if (struct.is_set_version()) {
          oprot.writeFieldBegin(VERSION_FIELD_DESC);
          oprot.writeString(struct.version);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SupervisorSummaryTupleSchemeFactory implements SchemeFactory {
    public SupervisorSummaryTupleScheme getScheme() {
      return new SupervisorSummaryTupleScheme();
    }
  }

  private static class SupervisorSummaryTupleScheme extends TupleScheme<SupervisorSummary> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SupervisorSummary struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.host);
      oprot.writeI32(struct.uptime_secs);
      oprot.writeI32(struct.num_workers);
      oprot.writeI32(struct.num_used_workers);
      oprot.writeString(struct.supervisor_id);
      BitSet optionals = new BitSet();
      if (struct.is_set_version()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.is_set_version()) {
        oprot.writeString(struct.version);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SupervisorSummary struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.host = iprot.readString();
      struct.set_host_isSet(true);
      struct.uptime_secs = iprot.readI32();
      struct.set_uptime_secs_isSet(true);
      struct.num_workers = iprot.readI32();
      struct.set_num_workers_isSet(true);
      struct.num_used_workers = iprot.readI32();
      struct.set_num_used_workers_isSet(true);
      struct.supervisor_id = iprot.readString();
      struct.set_supervisor_id_isSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.version = iprot.readString();
        struct.set_version_isSet(true);
      }
    }
  }

}

