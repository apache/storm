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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-2-18")
public class ExecutorStats implements org.apache.thrift.TBase<ExecutorStats, ExecutorStats._Fields>, java.io.Serializable, Cloneable, Comparable<ExecutorStats> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ExecutorStats");

  private static final org.apache.thrift.protocol.TField EMITTED_FIELD_DESC = new org.apache.thrift.protocol.TField("emitted", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField TRANSFERRED_FIELD_DESC = new org.apache.thrift.protocol.TField("transferred", org.apache.thrift.protocol.TType.MAP, (short)2);
  private static final org.apache.thrift.protocol.TField SPECIFIC_FIELD_DESC = new org.apache.thrift.protocol.TField("specific", org.apache.thrift.protocol.TType.STRUCT, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ExecutorStatsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ExecutorStatsTupleSchemeFactory());
  }

  private Map<String,Map<String,Long>> emitted; // required
  private Map<String,Map<String,Long>> transferred; // required
  private ExecutorSpecificStats specific; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    EMITTED((short)1, "emitted"),
    TRANSFERRED((short)2, "transferred"),
    SPECIFIC((short)3, "specific");

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
        case 1: // EMITTED
          return EMITTED;
        case 2: // TRANSFERRED
          return TRANSFERRED;
        case 3: // SPECIFIC
          return SPECIFIC;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.EMITTED, new org.apache.thrift.meta_data.FieldMetaData("emitted", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)))));
    tmpMap.put(_Fields.TRANSFERRED, new org.apache.thrift.meta_data.FieldMetaData("transferred", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)))));
    tmpMap.put(_Fields.SPECIFIC, new org.apache.thrift.meta_data.FieldMetaData("specific", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ExecutorSpecificStats.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ExecutorStats.class, metaDataMap);
  }

  public ExecutorStats() {
  }

  public ExecutorStats(
    Map<String,Map<String,Long>> emitted,
    Map<String,Map<String,Long>> transferred,
    ExecutorSpecificStats specific)
  {
    this();
    this.emitted = emitted;
    this.transferred = transferred;
    this.specific = specific;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ExecutorStats(ExecutorStats other) {
    if (other.is_set_emitted()) {
      Map<String,Map<String,Long>> __this__emitted = new HashMap<String,Map<String,Long>>(other.emitted.size());
      for (Map.Entry<String, Map<String,Long>> other_element : other.emitted.entrySet()) {

        String other_element_key = other_element.getKey();
        Map<String,Long> other_element_value = other_element.getValue();

        String __this__emitted_copy_key = other_element_key;

        Map<String,Long> __this__emitted_copy_value = new HashMap<String,Long>(other_element_value);

        __this__emitted.put(__this__emitted_copy_key, __this__emitted_copy_value);
      }
      this.emitted = __this__emitted;
    }
    if (other.is_set_transferred()) {
      Map<String,Map<String,Long>> __this__transferred = new HashMap<String,Map<String,Long>>(other.transferred.size());
      for (Map.Entry<String, Map<String,Long>> other_element : other.transferred.entrySet()) {

        String other_element_key = other_element.getKey();
        Map<String,Long> other_element_value = other_element.getValue();

        String __this__transferred_copy_key = other_element_key;

        Map<String,Long> __this__transferred_copy_value = new HashMap<String,Long>(other_element_value);

        __this__transferred.put(__this__transferred_copy_key, __this__transferred_copy_value);
      }
      this.transferred = __this__transferred;
    }
    if (other.is_set_specific()) {
      this.specific = new ExecutorSpecificStats(other.specific);
    }
  }

  public ExecutorStats deepCopy() {
    return new ExecutorStats(this);
  }

  @Override
  public void clear() {
    this.emitted = null;
    this.transferred = null;
    this.specific = null;
  }

  public int get_emitted_size() {
    return (this.emitted == null) ? 0 : this.emitted.size();
  }

  public void put_to_emitted(String key, Map<String,Long> val) {
    if (this.emitted == null) {
      this.emitted = new HashMap<String,Map<String,Long>>();
    }
    this.emitted.put(key, val);
  }

  public Map<String,Map<String,Long>> get_emitted() {
    return this.emitted;
  }

  public void set_emitted(Map<String,Map<String,Long>> emitted) {
    this.emitted = emitted;
  }

  public void unset_emitted() {
    this.emitted = null;
  }

  /** Returns true if field emitted is set (has been assigned a value) and false otherwise */
  public boolean is_set_emitted() {
    return this.emitted != null;
  }

  public void set_emitted_isSet(boolean value) {
    if (!value) {
      this.emitted = null;
    }
  }

  public int get_transferred_size() {
    return (this.transferred == null) ? 0 : this.transferred.size();
  }

  public void put_to_transferred(String key, Map<String,Long> val) {
    if (this.transferred == null) {
      this.transferred = new HashMap<String,Map<String,Long>>();
    }
    this.transferred.put(key, val);
  }

  public Map<String,Map<String,Long>> get_transferred() {
    return this.transferred;
  }

  public void set_transferred(Map<String,Map<String,Long>> transferred) {
    this.transferred = transferred;
  }

  public void unset_transferred() {
    this.transferred = null;
  }

  /** Returns true if field transferred is set (has been assigned a value) and false otherwise */
  public boolean is_set_transferred() {
    return this.transferred != null;
  }

  public void set_transferred_isSet(boolean value) {
    if (!value) {
      this.transferred = null;
    }
  }

  public ExecutorSpecificStats get_specific() {
    return this.specific;
  }

  public void set_specific(ExecutorSpecificStats specific) {
    this.specific = specific;
  }

  public void unset_specific() {
    this.specific = null;
  }

  /** Returns true if field specific is set (has been assigned a value) and false otherwise */
  public boolean is_set_specific() {
    return this.specific != null;
  }

  public void set_specific_isSet(boolean value) {
    if (!value) {
      this.specific = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case EMITTED:
      if (value == null) {
        unset_emitted();
      } else {
        set_emitted((Map<String,Map<String,Long>>)value);
      }
      break;

    case TRANSFERRED:
      if (value == null) {
        unset_transferred();
      } else {
        set_transferred((Map<String,Map<String,Long>>)value);
      }
      break;

    case SPECIFIC:
      if (value == null) {
        unset_specific();
      } else {
        set_specific((ExecutorSpecificStats)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case EMITTED:
      return get_emitted();

    case TRANSFERRED:
      return get_transferred();

    case SPECIFIC:
      return get_specific();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case EMITTED:
      return is_set_emitted();
    case TRANSFERRED:
      return is_set_transferred();
    case SPECIFIC:
      return is_set_specific();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ExecutorStats)
      return this.equals((ExecutorStats)that);
    return false;
  }

  public boolean equals(ExecutorStats that) {
    if (that == null)
      return false;

    boolean this_present_emitted = true && this.is_set_emitted();
    boolean that_present_emitted = true && that.is_set_emitted();
    if (this_present_emitted || that_present_emitted) {
      if (!(this_present_emitted && that_present_emitted))
        return false;
      if (!this.emitted.equals(that.emitted))
        return false;
    }

    boolean this_present_transferred = true && this.is_set_transferred();
    boolean that_present_transferred = true && that.is_set_transferred();
    if (this_present_transferred || that_present_transferred) {
      if (!(this_present_transferred && that_present_transferred))
        return false;
      if (!this.transferred.equals(that.transferred))
        return false;
    }

    boolean this_present_specific = true && this.is_set_specific();
    boolean that_present_specific = true && that.is_set_specific();
    if (this_present_specific || that_present_specific) {
      if (!(this_present_specific && that_present_specific))
        return false;
      if (!this.specific.equals(that.specific))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_emitted = true && (is_set_emitted());
    list.add(present_emitted);
    if (present_emitted)
      list.add(emitted);

    boolean present_transferred = true && (is_set_transferred());
    list.add(present_transferred);
    if (present_transferred)
      list.add(transferred);

    boolean present_specific = true && (is_set_specific());
    list.add(present_specific);
    if (present_specific)
      list.add(specific);

    return list.hashCode();
  }

  @Override
  public int compareTo(ExecutorStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_emitted()).compareTo(other.is_set_emitted());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_emitted()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.emitted, other.emitted);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_transferred()).compareTo(other.is_set_transferred());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_transferred()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.transferred, other.transferred);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_specific()).compareTo(other.is_set_specific());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_specific()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.specific, other.specific);
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
    StringBuilder sb = new StringBuilder("ExecutorStats(");
    boolean first = true;

    sb.append("emitted:");
    if (this.emitted == null) {
      sb.append("null");
    } else {
      sb.append(this.emitted);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("transferred:");
    if (this.transferred == null) {
      sb.append("null");
    } else {
      sb.append(this.transferred);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("specific:");
    if (this.specific == null) {
      sb.append("null");
    } else {
      sb.append(this.specific);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_emitted()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'emitted' is unset! Struct:" + toString());
    }

    if (!is_set_transferred()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'transferred' is unset! Struct:" + toString());
    }

    if (!is_set_specific()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'specific' is unset! Struct:" + toString());
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ExecutorStatsStandardSchemeFactory implements SchemeFactory {
    public ExecutorStatsStandardScheme getScheme() {
      return new ExecutorStatsStandardScheme();
    }
  }

  private static class ExecutorStatsStandardScheme extends StandardScheme<ExecutorStats> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ExecutorStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // EMITTED
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map330 = iprot.readMapBegin();
                struct.emitted = new HashMap<String,Map<String,Long>>(2*_map330.size);
                String _key331;
                Map<String,Long> _val332;
                for (int _i333 = 0; _i333 < _map330.size; ++_i333)
                {
                  _key331 = iprot.readString();
                  {
                    org.apache.thrift.protocol.TMap _map334 = iprot.readMapBegin();
                    _val332 = new HashMap<String,Long>(2*_map334.size);
                    String _key335;
                    long _val336;
                    for (int _i337 = 0; _i337 < _map334.size; ++_i337)
                    {
                      _key335 = iprot.readString();
                      _val336 = iprot.readI64();
                      _val332.put(_key335, _val336);
                    }
                    iprot.readMapEnd();
                  }
                  struct.emitted.put(_key331, _val332);
                }
                iprot.readMapEnd();
              }
              struct.set_emitted_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TRANSFERRED
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map338 = iprot.readMapBegin();
                struct.transferred = new HashMap<String,Map<String,Long>>(2*_map338.size);
                String _key339;
                Map<String,Long> _val340;
                for (int _i341 = 0; _i341 < _map338.size; ++_i341)
                {
                  _key339 = iprot.readString();
                  {
                    org.apache.thrift.protocol.TMap _map342 = iprot.readMapBegin();
                    _val340 = new HashMap<String,Long>(2*_map342.size);
                    String _key343;
                    long _val344;
                    for (int _i345 = 0; _i345 < _map342.size; ++_i345)
                    {
                      _key343 = iprot.readString();
                      _val344 = iprot.readI64();
                      _val340.put(_key343, _val344);
                    }
                    iprot.readMapEnd();
                  }
                  struct.transferred.put(_key339, _val340);
                }
                iprot.readMapEnd();
              }
              struct.set_transferred_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SPECIFIC
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.specific = new ExecutorSpecificStats();
              struct.specific.read(iprot);
              struct.set_specific_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ExecutorStats struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.emitted != null) {
        oprot.writeFieldBegin(EMITTED_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.MAP, struct.emitted.size()));
          for (Map.Entry<String, Map<String,Long>> _iter346 : struct.emitted.entrySet())
          {
            oprot.writeString(_iter346.getKey());
            {
              oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, _iter346.getValue().size()));
              for (Map.Entry<String, Long> _iter347 : _iter346.getValue().entrySet())
              {
                oprot.writeString(_iter347.getKey());
                oprot.writeI64(_iter347.getValue());
              }
              oprot.writeMapEnd();
            }
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.transferred != null) {
        oprot.writeFieldBegin(TRANSFERRED_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.MAP, struct.transferred.size()));
          for (Map.Entry<String, Map<String,Long>> _iter348 : struct.transferred.entrySet())
          {
            oprot.writeString(_iter348.getKey());
            {
              oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, _iter348.getValue().size()));
              for (Map.Entry<String, Long> _iter349 : _iter348.getValue().entrySet())
              {
                oprot.writeString(_iter349.getKey());
                oprot.writeI64(_iter349.getValue());
              }
              oprot.writeMapEnd();
            }
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.specific != null) {
        oprot.writeFieldBegin(SPECIFIC_FIELD_DESC);
        struct.specific.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ExecutorStatsTupleSchemeFactory implements SchemeFactory {
    public ExecutorStatsTupleScheme getScheme() {
      return new ExecutorStatsTupleScheme();
    }
  }

  private static class ExecutorStatsTupleScheme extends TupleScheme<ExecutorStats> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ExecutorStats struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.emitted.size());
        for (Map.Entry<String, Map<String,Long>> _iter350 : struct.emitted.entrySet())
        {
          oprot.writeString(_iter350.getKey());
          {
            oprot.writeI32(_iter350.getValue().size());
            for (Map.Entry<String, Long> _iter351 : _iter350.getValue().entrySet())
            {
              oprot.writeString(_iter351.getKey());
              oprot.writeI64(_iter351.getValue());
            }
          }
        }
      }
      {
        oprot.writeI32(struct.transferred.size());
        for (Map.Entry<String, Map<String,Long>> _iter352 : struct.transferred.entrySet())
        {
          oprot.writeString(_iter352.getKey());
          {
            oprot.writeI32(_iter352.getValue().size());
            for (Map.Entry<String, Long> _iter353 : _iter352.getValue().entrySet())
            {
              oprot.writeString(_iter353.getKey());
              oprot.writeI64(_iter353.getValue());
            }
          }
        }
      }
      struct.specific.write(oprot);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ExecutorStats struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map354 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.MAP, iprot.readI32());
        struct.emitted = new HashMap<String,Map<String,Long>>(2*_map354.size);
        String _key355;
        Map<String,Long> _val356;
        for (int _i357 = 0; _i357 < _map354.size; ++_i357)
        {
          _key355 = iprot.readString();
          {
            org.apache.thrift.protocol.TMap _map358 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, iprot.readI32());
            _val356 = new HashMap<String,Long>(2*_map358.size);
            String _key359;
            long _val360;
            for (int _i361 = 0; _i361 < _map358.size; ++_i361)
            {
              _key359 = iprot.readString();
              _val360 = iprot.readI64();
              _val356.put(_key359, _val360);
            }
          }
          struct.emitted.put(_key355, _val356);
        }
      }
      struct.set_emitted_isSet(true);
      {
        org.apache.thrift.protocol.TMap _map362 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.MAP, iprot.readI32());
        struct.transferred = new HashMap<String,Map<String,Long>>(2*_map362.size);
        String _key363;
        Map<String,Long> _val364;
        for (int _i365 = 0; _i365 < _map362.size; ++_i365)
        {
          _key363 = iprot.readString();
          {
            org.apache.thrift.protocol.TMap _map366 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, iprot.readI32());
            _val364 = new HashMap<String,Long>(2*_map366.size);
            String _key367;
            long _val368;
            for (int _i369 = 0; _i369 < _map366.size; ++_i369)
            {
              _key367 = iprot.readString();
              _val368 = iprot.readI64();
              _val364.put(_key367, _val368);
            }
          }
          struct.transferred.put(_key363, _val364);
        }
      }
      struct.set_transferred_isSet(true);
      struct.specific = new ExecutorSpecificStats();
      struct.specific.read(iprot);
      struct.set_specific_isSet(true);
    }
  }

}

