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
public class DRPCRequest implements org.apache.thrift.TBase<DRPCRequest, DRPCRequest._Fields>, java.io.Serializable, Cloneable, Comparable<DRPCRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("DRPCRequest");

  private static final org.apache.thrift.protocol.TField FUNC_ARGS_FIELD_DESC = new org.apache.thrift.protocol.TField("func_args", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField REQUEST_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("request_id", org.apache.thrift.protocol.TType.STRING, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new DRPCRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new DRPCRequestTupleSchemeFactory());
  }

  private String func_args; // required
  private String request_id; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FUNC_ARGS((short)1, "func_args"),
    REQUEST_ID((short)2, "request_id");

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
        case 1: // FUNC_ARGS
          return FUNC_ARGS;
        case 2: // REQUEST_ID
          return REQUEST_ID;
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
    tmpMap.put(_Fields.FUNC_ARGS, new org.apache.thrift.meta_data.FieldMetaData("func_args", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.REQUEST_ID, new org.apache.thrift.meta_data.FieldMetaData("request_id", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(DRPCRequest.class, metaDataMap);
  }

  public DRPCRequest() {
  }

  public DRPCRequest(
    String func_args,
    String request_id)
  {
    this();
    this.func_args = func_args;
    this.request_id = request_id;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public DRPCRequest(DRPCRequest other) {
    if (other.is_set_func_args()) {
      this.func_args = other.func_args;
    }
    if (other.is_set_request_id()) {
      this.request_id = other.request_id;
    }
  }

  public DRPCRequest deepCopy() {
    return new DRPCRequest(this);
  }

  @Override
  public void clear() {
    this.func_args = null;
    this.request_id = null;
  }

  public String get_func_args() {
    return this.func_args;
  }

  public void set_func_args(String func_args) {
    this.func_args = func_args;
  }

  public void unset_func_args() {
    this.func_args = null;
  }

  /** Returns true if field func_args is set (has been assigned a value) and false otherwise */
  public boolean is_set_func_args() {
    return this.func_args != null;
  }

  public void set_func_args_isSet(boolean value) {
    if (!value) {
      this.func_args = null;
    }
  }

  public String get_request_id() {
    return this.request_id;
  }

  public void set_request_id(String request_id) {
    this.request_id = request_id;
  }

  public void unset_request_id() {
    this.request_id = null;
  }

  /** Returns true if field request_id is set (has been assigned a value) and false otherwise */
  public boolean is_set_request_id() {
    return this.request_id != null;
  }

  public void set_request_id_isSet(boolean value) {
    if (!value) {
      this.request_id = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FUNC_ARGS:
      if (value == null) {
        unset_func_args();
      } else {
        set_func_args((String)value);
      }
      break;

    case REQUEST_ID:
      if (value == null) {
        unset_request_id();
      } else {
        set_request_id((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FUNC_ARGS:
      return get_func_args();

    case REQUEST_ID:
      return get_request_id();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FUNC_ARGS:
      return is_set_func_args();
    case REQUEST_ID:
      return is_set_request_id();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof DRPCRequest)
      return this.equals((DRPCRequest)that);
    return false;
  }

  public boolean equals(DRPCRequest that) {
    if (that == null)
      return false;

    boolean this_present_func_args = true && this.is_set_func_args();
    boolean that_present_func_args = true && that.is_set_func_args();
    if (this_present_func_args || that_present_func_args) {
      if (!(this_present_func_args && that_present_func_args))
        return false;
      if (!this.func_args.equals(that.func_args))
        return false;
    }

    boolean this_present_request_id = true && this.is_set_request_id();
    boolean that_present_request_id = true && that.is_set_request_id();
    if (this_present_request_id || that_present_request_id) {
      if (!(this_present_request_id && that_present_request_id))
        return false;
      if (!this.request_id.equals(that.request_id))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_func_args = true && (is_set_func_args());
    list.add(present_func_args);
    if (present_func_args)
      list.add(func_args);

    boolean present_request_id = true && (is_set_request_id());
    list.add(present_request_id);
    if (present_request_id)
      list.add(request_id);

    return list.hashCode();
  }

  @Override
  public int compareTo(DRPCRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_func_args()).compareTo(other.is_set_func_args());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_func_args()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.func_args, other.func_args);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_request_id()).compareTo(other.is_set_request_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_request_id()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.request_id, other.request_id);
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
    StringBuilder sb = new StringBuilder("DRPCRequest(");
    boolean first = true;

    sb.append("func_args:");
    if (this.func_args == null) {
      sb.append("null");
    } else {
      sb.append(this.func_args);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("request_id:");
    if (this.request_id == null) {
      sb.append("null");
    } else {
      sb.append(this.request_id);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_func_args()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'func_args' is unset! Struct:" + toString());
    }

    if (!is_set_request_id()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'request_id' is unset! Struct:" + toString());
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

  private static class DRPCRequestStandardSchemeFactory implements SchemeFactory {
    public DRPCRequestStandardScheme getScheme() {
      return new DRPCRequestStandardScheme();
    }
  }

  private static class DRPCRequestStandardScheme extends StandardScheme<DRPCRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, DRPCRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FUNC_ARGS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.func_args = iprot.readString();
              struct.set_func_args_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // REQUEST_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.request_id = iprot.readString();
              struct.set_request_id_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, DRPCRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.func_args != null) {
        oprot.writeFieldBegin(FUNC_ARGS_FIELD_DESC);
        oprot.writeString(struct.func_args);
        oprot.writeFieldEnd();
      }
      if (struct.request_id != null) {
        oprot.writeFieldBegin(REQUEST_ID_FIELD_DESC);
        oprot.writeString(struct.request_id);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class DRPCRequestTupleSchemeFactory implements SchemeFactory {
    public DRPCRequestTupleScheme getScheme() {
      return new DRPCRequestTupleScheme();
    }
  }

  private static class DRPCRequestTupleScheme extends TupleScheme<DRPCRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, DRPCRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.func_args);
      oprot.writeString(struct.request_id);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, DRPCRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.func_args = iprot.readString();
      struct.set_func_args_isSet(true);
      struct.request_id = iprot.readString();
      struct.set_request_id_isSet(true);
    }
  }

}

