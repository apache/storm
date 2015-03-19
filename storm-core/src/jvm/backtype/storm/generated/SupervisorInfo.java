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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-3-5")
public class SupervisorInfo implements org.apache.thrift.TBase<SupervisorInfo, SupervisorInfo._Fields>, java.io.Serializable, Cloneable, Comparable<SupervisorInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SupervisorInfo");

  private static final org.apache.thrift.protocol.TField TIME_SECS_FIELD_DESC = new org.apache.thrift.protocol.TField("time_secs", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField HOSTNAME_FIELD_DESC = new org.apache.thrift.protocol.TField("hostname", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField ASSIGNMENT_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("assignment_id", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField USED_PORTS_FIELD_DESC = new org.apache.thrift.protocol.TField("used_ports", org.apache.thrift.protocol.TType.LIST, (short)4);
  private static final org.apache.thrift.protocol.TField META_FIELD_DESC = new org.apache.thrift.protocol.TField("meta", org.apache.thrift.protocol.TType.LIST, (short)5);
  private static final org.apache.thrift.protocol.TField SCHEDULER_META_FIELD_DESC = new org.apache.thrift.protocol.TField("scheduler_meta", org.apache.thrift.protocol.TType.MAP, (short)6);
  private static final org.apache.thrift.protocol.TField UPTIME_SECS_FIELD_DESC = new org.apache.thrift.protocol.TField("uptime_secs", org.apache.thrift.protocol.TType.I64, (short)7);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SupervisorInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SupervisorInfoTupleSchemeFactory());
  }

  private long time_secs; // required
  private String hostname; // required
  private String assignment_id; // optional
  private List<Long> used_ports; // optional
  private List<Long> meta; // optional
  private Map<String,String> scheduler_meta; // optional
  private long uptime_secs; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TIME_SECS((short)1, "time_secs"),
    HOSTNAME((short)2, "hostname"),
    ASSIGNMENT_ID((short)3, "assignment_id"),
    USED_PORTS((short)4, "used_ports"),
    META((short)5, "meta"),
    SCHEDULER_META((short)6, "scheduler_meta"),
    UPTIME_SECS((short)7, "uptime_secs");

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
        case 1: // TIME_SECS
          return TIME_SECS;
        case 2: // HOSTNAME
          return HOSTNAME;
        case 3: // ASSIGNMENT_ID
          return ASSIGNMENT_ID;
        case 4: // USED_PORTS
          return USED_PORTS;
        case 5: // META
          return META;
        case 6: // SCHEDULER_META
          return SCHEDULER_META;
        case 7: // UPTIME_SECS
          return UPTIME_SECS;
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
  private static final int __TIME_SECS_ISSET_ID = 0;
  private static final int __UPTIME_SECS_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.ASSIGNMENT_ID,_Fields.USED_PORTS,_Fields.META,_Fields.SCHEDULER_META,_Fields.UPTIME_SECS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TIME_SECS, new org.apache.thrift.meta_data.FieldMetaData("time_secs", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.HOSTNAME, new org.apache.thrift.meta_data.FieldMetaData("hostname", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.ASSIGNMENT_ID, new org.apache.thrift.meta_data.FieldMetaData("assignment_id", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.USED_PORTS, new org.apache.thrift.meta_data.FieldMetaData("used_ports", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.META, new org.apache.thrift.meta_data.FieldMetaData("meta", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.SCHEDULER_META, new org.apache.thrift.meta_data.FieldMetaData("scheduler_meta", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.UPTIME_SECS, new org.apache.thrift.meta_data.FieldMetaData("uptime_secs", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SupervisorInfo.class, metaDataMap);
  }

  public SupervisorInfo() {
  }

  public SupervisorInfo(
    long time_secs,
    String hostname)
  {
    this();
    this.time_secs = time_secs;
    set_time_secs_isSet(true);
    this.hostname = hostname;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SupervisorInfo(SupervisorInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    this.time_secs = other.time_secs;
    if (other.is_set_hostname()) {
      this.hostname = other.hostname;
    }
    if (other.is_set_assignment_id()) {
      this.assignment_id = other.assignment_id;
    }
    if (other.is_set_used_ports()) {
      List<Long> __this__used_ports = new ArrayList<Long>(other.used_ports);
      this.used_ports = __this__used_ports;
    }
    if (other.is_set_meta()) {
      List<Long> __this__meta = new ArrayList<Long>(other.meta);
      this.meta = __this__meta;
    }
    if (other.is_set_scheduler_meta()) {
      Map<String,String> __this__scheduler_meta = new HashMap<String,String>(other.scheduler_meta);
      this.scheduler_meta = __this__scheduler_meta;
    }
    this.uptime_secs = other.uptime_secs;
  }

  public SupervisorInfo deepCopy() {
    return new SupervisorInfo(this);
  }

  @Override
  public void clear() {
    set_time_secs_isSet(false);
    this.time_secs = 0;
    this.hostname = null;
    this.assignment_id = null;
    this.used_ports = null;
    this.meta = null;
    this.scheduler_meta = null;
    set_uptime_secs_isSet(false);
    this.uptime_secs = 0;
  }

  public long get_time_secs() {
    return this.time_secs;
  }

  public void set_time_secs(long time_secs) {
    this.time_secs = time_secs;
    set_time_secs_isSet(true);
  }

  public void unset_time_secs() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TIME_SECS_ISSET_ID);
  }

  /** Returns true if field time_secs is set (has been assigned a value) and false otherwise */
  public boolean is_set_time_secs() {
    return EncodingUtils.testBit(__isset_bitfield, __TIME_SECS_ISSET_ID);
  }

  public void set_time_secs_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TIME_SECS_ISSET_ID, value);
  }

  public String get_hostname() {
    return this.hostname;
  }

  public void set_hostname(String hostname) {
    this.hostname = hostname;
  }

  public void unset_hostname() {
    this.hostname = null;
  }

  /** Returns true if field hostname is set (has been assigned a value) and false otherwise */
  public boolean is_set_hostname() {
    return this.hostname != null;
  }

  public void set_hostname_isSet(boolean value) {
    if (!value) {
      this.hostname = null;
    }
  }

  public String get_assignment_id() {
    return this.assignment_id;
  }

  public void set_assignment_id(String assignment_id) {
    this.assignment_id = assignment_id;
  }

  public void unset_assignment_id() {
    this.assignment_id = null;
  }

  /** Returns true if field assignment_id is set (has been assigned a value) and false otherwise */
  public boolean is_set_assignment_id() {
    return this.assignment_id != null;
  }

  public void set_assignment_id_isSet(boolean value) {
    if (!value) {
      this.assignment_id = null;
    }
  }

  public int get_used_ports_size() {
    return (this.used_ports == null) ? 0 : this.used_ports.size();
  }

  public java.util.Iterator<Long> get_used_ports_iterator() {
    return (this.used_ports == null) ? null : this.used_ports.iterator();
  }

  public void add_to_used_ports(long elem) {
    if (this.used_ports == null) {
      this.used_ports = new ArrayList<Long>();
    }
    this.used_ports.add(elem);
  }

  public List<Long> get_used_ports() {
    return this.used_ports;
  }

  public void set_used_ports(List<Long> used_ports) {
    this.used_ports = used_ports;
  }

  public void unset_used_ports() {
    this.used_ports = null;
  }

  /** Returns true if field used_ports is set (has been assigned a value) and false otherwise */
  public boolean is_set_used_ports() {
    return this.used_ports != null;
  }

  public void set_used_ports_isSet(boolean value) {
    if (!value) {
      this.used_ports = null;
    }
  }

  public int get_meta_size() {
    return (this.meta == null) ? 0 : this.meta.size();
  }

  public java.util.Iterator<Long> get_meta_iterator() {
    return (this.meta == null) ? null : this.meta.iterator();
  }

  public void add_to_meta(long elem) {
    if (this.meta == null) {
      this.meta = new ArrayList<Long>();
    }
    this.meta.add(elem);
  }

  public List<Long> get_meta() {
    return this.meta;
  }

  public void set_meta(List<Long> meta) {
    this.meta = meta;
  }

  public void unset_meta() {
    this.meta = null;
  }

  /** Returns true if field meta is set (has been assigned a value) and false otherwise */
  public boolean is_set_meta() {
    return this.meta != null;
  }

  public void set_meta_isSet(boolean value) {
    if (!value) {
      this.meta = null;
    }
  }

  public int get_scheduler_meta_size() {
    return (this.scheduler_meta == null) ? 0 : this.scheduler_meta.size();
  }

  public void put_to_scheduler_meta(String key, String val) {
    if (this.scheduler_meta == null) {
      this.scheduler_meta = new HashMap<String,String>();
    }
    this.scheduler_meta.put(key, val);
  }

  public Map<String,String> get_scheduler_meta() {
    return this.scheduler_meta;
  }

  public void set_scheduler_meta(Map<String,String> scheduler_meta) {
    this.scheduler_meta = scheduler_meta;
  }

  public void unset_scheduler_meta() {
    this.scheduler_meta = null;
  }

  /** Returns true if field scheduler_meta is set (has been assigned a value) and false otherwise */
  public boolean is_set_scheduler_meta() {
    return this.scheduler_meta != null;
  }

  public void set_scheduler_meta_isSet(boolean value) {
    if (!value) {
      this.scheduler_meta = null;
    }
  }

  public long get_uptime_secs() {
    return this.uptime_secs;
  }

  public void set_uptime_secs(long uptime_secs) {
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

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TIME_SECS:
      if (value == null) {
        unset_time_secs();
      } else {
        set_time_secs((Long)value);
      }
      break;

    case HOSTNAME:
      if (value == null) {
        unset_hostname();
      } else {
        set_hostname((String)value);
      }
      break;

    case ASSIGNMENT_ID:
      if (value == null) {
        unset_assignment_id();
      } else {
        set_assignment_id((String)value);
      }
      break;

    case USED_PORTS:
      if (value == null) {
        unset_used_ports();
      } else {
        set_used_ports((List<Long>)value);
      }
      break;

    case META:
      if (value == null) {
        unset_meta();
      } else {
        set_meta((List<Long>)value);
      }
      break;

    case SCHEDULER_META:
      if (value == null) {
        unset_scheduler_meta();
      } else {
        set_scheduler_meta((Map<String,String>)value);
      }
      break;

    case UPTIME_SECS:
      if (value == null) {
        unset_uptime_secs();
      } else {
        set_uptime_secs((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TIME_SECS:
      return Long.valueOf(get_time_secs());

    case HOSTNAME:
      return get_hostname();

    case ASSIGNMENT_ID:
      return get_assignment_id();

    case USED_PORTS:
      return get_used_ports();

    case META:
      return get_meta();

    case SCHEDULER_META:
      return get_scheduler_meta();

    case UPTIME_SECS:
      return Long.valueOf(get_uptime_secs());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TIME_SECS:
      return is_set_time_secs();
    case HOSTNAME:
      return is_set_hostname();
    case ASSIGNMENT_ID:
      return is_set_assignment_id();
    case USED_PORTS:
      return is_set_used_ports();
    case META:
      return is_set_meta();
    case SCHEDULER_META:
      return is_set_scheduler_meta();
    case UPTIME_SECS:
      return is_set_uptime_secs();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SupervisorInfo)
      return this.equals((SupervisorInfo)that);
    return false;
  }

  public boolean equals(SupervisorInfo that) {
    if (that == null)
      return false;

    boolean this_present_time_secs = true;
    boolean that_present_time_secs = true;
    if (this_present_time_secs || that_present_time_secs) {
      if (!(this_present_time_secs && that_present_time_secs))
        return false;
      if (this.time_secs != that.time_secs)
        return false;
    }

    boolean this_present_hostname = true && this.is_set_hostname();
    boolean that_present_hostname = true && that.is_set_hostname();
    if (this_present_hostname || that_present_hostname) {
      if (!(this_present_hostname && that_present_hostname))
        return false;
      if (!this.hostname.equals(that.hostname))
        return false;
    }

    boolean this_present_assignment_id = true && this.is_set_assignment_id();
    boolean that_present_assignment_id = true && that.is_set_assignment_id();
    if (this_present_assignment_id || that_present_assignment_id) {
      if (!(this_present_assignment_id && that_present_assignment_id))
        return false;
      if (!this.assignment_id.equals(that.assignment_id))
        return false;
    }

    boolean this_present_used_ports = true && this.is_set_used_ports();
    boolean that_present_used_ports = true && that.is_set_used_ports();
    if (this_present_used_ports || that_present_used_ports) {
      if (!(this_present_used_ports && that_present_used_ports))
        return false;
      if (!this.used_ports.equals(that.used_ports))
        return false;
    }

    boolean this_present_meta = true && this.is_set_meta();
    boolean that_present_meta = true && that.is_set_meta();
    if (this_present_meta || that_present_meta) {
      if (!(this_present_meta && that_present_meta))
        return false;
      if (!this.meta.equals(that.meta))
        return false;
    }

    boolean this_present_scheduler_meta = true && this.is_set_scheduler_meta();
    boolean that_present_scheduler_meta = true && that.is_set_scheduler_meta();
    if (this_present_scheduler_meta || that_present_scheduler_meta) {
      if (!(this_present_scheduler_meta && that_present_scheduler_meta))
        return false;
      if (!this.scheduler_meta.equals(that.scheduler_meta))
        return false;
    }

    boolean this_present_uptime_secs = true && this.is_set_uptime_secs();
    boolean that_present_uptime_secs = true && that.is_set_uptime_secs();
    if (this_present_uptime_secs || that_present_uptime_secs) {
      if (!(this_present_uptime_secs && that_present_uptime_secs))
        return false;
      if (this.uptime_secs != that.uptime_secs)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_time_secs = true;
    list.add(present_time_secs);
    if (present_time_secs)
      list.add(time_secs);

    boolean present_hostname = true && (is_set_hostname());
    list.add(present_hostname);
    if (present_hostname)
      list.add(hostname);

    boolean present_assignment_id = true && (is_set_assignment_id());
    list.add(present_assignment_id);
    if (present_assignment_id)
      list.add(assignment_id);

    boolean present_used_ports = true && (is_set_used_ports());
    list.add(present_used_ports);
    if (present_used_ports)
      list.add(used_ports);

    boolean present_meta = true && (is_set_meta());
    list.add(present_meta);
    if (present_meta)
      list.add(meta);

    boolean present_scheduler_meta = true && (is_set_scheduler_meta());
    list.add(present_scheduler_meta);
    if (present_scheduler_meta)
      list.add(scheduler_meta);

    boolean present_uptime_secs = true && (is_set_uptime_secs());
    list.add(present_uptime_secs);
    if (present_uptime_secs)
      list.add(uptime_secs);

    return list.hashCode();
  }

  @Override
  public int compareTo(SupervisorInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_time_secs()).compareTo(other.is_set_time_secs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_time_secs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.time_secs, other.time_secs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_hostname()).compareTo(other.is_set_hostname());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_hostname()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.hostname, other.hostname);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_assignment_id()).compareTo(other.is_set_assignment_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_assignment_id()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.assignment_id, other.assignment_id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_used_ports()).compareTo(other.is_set_used_ports());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_used_ports()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.used_ports, other.used_ports);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_meta()).compareTo(other.is_set_meta());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_meta()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.meta, other.meta);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_scheduler_meta()).compareTo(other.is_set_scheduler_meta());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_scheduler_meta()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.scheduler_meta, other.scheduler_meta);
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
    StringBuilder sb = new StringBuilder("SupervisorInfo(");
    boolean first = true;

    sb.append("time_secs:");
    sb.append(this.time_secs);
    first = false;
    if (!first) sb.append(", ");
    sb.append("hostname:");
    if (this.hostname == null) {
      sb.append("null");
    } else {
      sb.append(this.hostname);
    }
    first = false;
    if (is_set_assignment_id()) {
      if (!first) sb.append(", ");
      sb.append("assignment_id:");
      if (this.assignment_id == null) {
        sb.append("null");
      } else {
        sb.append(this.assignment_id);
      }
      first = false;
    }
    if (is_set_used_ports()) {
      if (!first) sb.append(", ");
      sb.append("used_ports:");
      if (this.used_ports == null) {
        sb.append("null");
      } else {
        sb.append(this.used_ports);
      }
      first = false;
    }
    if (is_set_meta()) {
      if (!first) sb.append(", ");
      sb.append("meta:");
      if (this.meta == null) {
        sb.append("null");
      } else {
        sb.append(this.meta);
      }
      first = false;
    }
    if (is_set_scheduler_meta()) {
      if (!first) sb.append(", ");
      sb.append("scheduler_meta:");
      if (this.scheduler_meta == null) {
        sb.append("null");
      } else {
        sb.append(this.scheduler_meta);
      }
      first = false;
    }
    if (is_set_uptime_secs()) {
      if (!first) sb.append(", ");
      sb.append("uptime_secs:");
      sb.append(this.uptime_secs);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_time_secs()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'time_secs' is unset! Struct:" + toString());
    }

    if (!is_set_hostname()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'hostname' is unset! Struct:" + toString());
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

  private static class SupervisorInfoStandardSchemeFactory implements SchemeFactory {
    public SupervisorInfoStandardScheme getScheme() {
      return new SupervisorInfoStandardScheme();
    }
  }

  private static class SupervisorInfoStandardScheme extends StandardScheme<SupervisorInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SupervisorInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TIME_SECS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.time_secs = iprot.readI64();
              struct.set_time_secs_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // HOSTNAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.hostname = iprot.readString();
              struct.set_hostname_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // ASSIGNMENT_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.assignment_id = iprot.readString();
              struct.set_assignment_id_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // USED_PORTS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list336 = iprot.readListBegin();
                struct.used_ports = new ArrayList<Long>(_list336.size);
                long _elem337;
                for (int _i338 = 0; _i338 < _list336.size; ++_i338)
                {
                  _elem337 = iprot.readI64();
                  struct.used_ports.add(_elem337);
                }
                iprot.readListEnd();
              }
              struct.set_used_ports_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // META
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list339 = iprot.readListBegin();
                struct.meta = new ArrayList<Long>(_list339.size);
                long _elem340;
                for (int _i341 = 0; _i341 < _list339.size; ++_i341)
                {
                  _elem340 = iprot.readI64();
                  struct.meta.add(_elem340);
                }
                iprot.readListEnd();
              }
              struct.set_meta_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // SCHEDULER_META
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map342 = iprot.readMapBegin();
                struct.scheduler_meta = new HashMap<String,String>(2*_map342.size);
                String _key343;
                String _val344;
                for (int _i345 = 0; _i345 < _map342.size; ++_i345)
                {
                  _key343 = iprot.readString();
                  _val344 = iprot.readString();
                  struct.scheduler_meta.put(_key343, _val344);
                }
                iprot.readMapEnd();
              }
              struct.set_scheduler_meta_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // UPTIME_SECS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.uptime_secs = iprot.readI64();
              struct.set_uptime_secs_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, SupervisorInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(TIME_SECS_FIELD_DESC);
      oprot.writeI64(struct.time_secs);
      oprot.writeFieldEnd();
      if (struct.hostname != null) {
        oprot.writeFieldBegin(HOSTNAME_FIELD_DESC);
        oprot.writeString(struct.hostname);
        oprot.writeFieldEnd();
      }
      if (struct.assignment_id != null) {
        if (struct.is_set_assignment_id()) {
          oprot.writeFieldBegin(ASSIGNMENT_ID_FIELD_DESC);
          oprot.writeString(struct.assignment_id);
          oprot.writeFieldEnd();
        }
      }
      if (struct.used_ports != null) {
        if (struct.is_set_used_ports()) {
          oprot.writeFieldBegin(USED_PORTS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, struct.used_ports.size()));
            for (long _iter346 : struct.used_ports)
            {
              oprot.writeI64(_iter346);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.meta != null) {
        if (struct.is_set_meta()) {
          oprot.writeFieldBegin(META_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, struct.meta.size()));
            for (long _iter347 : struct.meta)
            {
              oprot.writeI64(_iter347);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.scheduler_meta != null) {
        if (struct.is_set_scheduler_meta()) {
          oprot.writeFieldBegin(SCHEDULER_META_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.scheduler_meta.size()));
            for (Map.Entry<String, String> _iter348 : struct.scheduler_meta.entrySet())
            {
              oprot.writeString(_iter348.getKey());
              oprot.writeString(_iter348.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.is_set_uptime_secs()) {
        oprot.writeFieldBegin(UPTIME_SECS_FIELD_DESC);
        oprot.writeI64(struct.uptime_secs);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SupervisorInfoTupleSchemeFactory implements SchemeFactory {
    public SupervisorInfoTupleScheme getScheme() {
      return new SupervisorInfoTupleScheme();
    }
  }

  private static class SupervisorInfoTupleScheme extends TupleScheme<SupervisorInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SupervisorInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI64(struct.time_secs);
      oprot.writeString(struct.hostname);
      BitSet optionals = new BitSet();
      if (struct.is_set_assignment_id()) {
        optionals.set(0);
      }
      if (struct.is_set_used_ports()) {
        optionals.set(1);
      }
      if (struct.is_set_meta()) {
        optionals.set(2);
      }
      if (struct.is_set_scheduler_meta()) {
        optionals.set(3);
      }
      if (struct.is_set_uptime_secs()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.is_set_assignment_id()) {
        oprot.writeString(struct.assignment_id);
      }
      if (struct.is_set_used_ports()) {
        {
          oprot.writeI32(struct.used_ports.size());
          for (long _iter349 : struct.used_ports)
          {
            oprot.writeI64(_iter349);
          }
        }
      }
      if (struct.is_set_meta()) {
        {
          oprot.writeI32(struct.meta.size());
          for (long _iter350 : struct.meta)
          {
            oprot.writeI64(_iter350);
          }
        }
      }
      if (struct.is_set_scheduler_meta()) {
        {
          oprot.writeI32(struct.scheduler_meta.size());
          for (Map.Entry<String, String> _iter351 : struct.scheduler_meta.entrySet())
          {
            oprot.writeString(_iter351.getKey());
            oprot.writeString(_iter351.getValue());
          }
        }
      }
      if (struct.is_set_uptime_secs()) {
        oprot.writeI64(struct.uptime_secs);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SupervisorInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.time_secs = iprot.readI64();
      struct.set_time_secs_isSet(true);
      struct.hostname = iprot.readString();
      struct.set_hostname_isSet(true);
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.assignment_id = iprot.readString();
        struct.set_assignment_id_isSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list352 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.used_ports = new ArrayList<Long>(_list352.size);
          long _elem353;
          for (int _i354 = 0; _i354 < _list352.size; ++_i354)
          {
            _elem353 = iprot.readI64();
            struct.used_ports.add(_elem353);
          }
        }
        struct.set_used_ports_isSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list355 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.meta = new ArrayList<Long>(_list355.size);
          long _elem356;
          for (int _i357 = 0; _i357 < _list355.size; ++_i357)
          {
            _elem356 = iprot.readI64();
            struct.meta.add(_elem356);
          }
        }
        struct.set_meta_isSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TMap _map358 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.scheduler_meta = new HashMap<String,String>(2*_map358.size);
          String _key359;
          String _val360;
          for (int _i361 = 0; _i361 < _map358.size; ++_i361)
          {
            _key359 = iprot.readString();
            _val360 = iprot.readString();
            struct.scheduler_meta.put(_key359, _val360);
          }
        }
        struct.set_scheduler_meta_isSet(true);
      }
      if (incoming.get(4)) {
        struct.uptime_secs = iprot.readI64();
        struct.set_uptime_secs_isSet(true);
      }
    }
  }

}

