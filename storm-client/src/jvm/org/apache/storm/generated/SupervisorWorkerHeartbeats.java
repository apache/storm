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
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.storm.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)")
public class SupervisorWorkerHeartbeats implements org.apache.storm.thrift.TBase<SupervisorWorkerHeartbeats, SupervisorWorkerHeartbeats._Fields>, java.io.Serializable, Cloneable, Comparable<SupervisorWorkerHeartbeats> {
  private static final org.apache.storm.thrift.protocol.TStruct STRUCT_DESC = new org.apache.storm.thrift.protocol.TStruct("SupervisorWorkerHeartbeats");

  private static final org.apache.storm.thrift.protocol.TField SUPERVISOR_ID_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("supervisor_id", org.apache.storm.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.storm.thrift.protocol.TField WORKER_HEARTBEATS_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("worker_heartbeats", org.apache.storm.thrift.protocol.TType.LIST, (short)2);

  private static final org.apache.storm.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new SupervisorWorkerHeartbeatsStandardSchemeFactory();
  private static final org.apache.storm.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new SupervisorWorkerHeartbeatsTupleSchemeFactory();

  private @org.apache.storm.thrift.annotation.Nullable java.lang.String supervisor_id; // required
  private @org.apache.storm.thrift.annotation.Nullable java.util.List<SupervisorWorkerHeartbeat> worker_heartbeats; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.storm.thrift.TFieldIdEnum {
    SUPERVISOR_ID((short)1, "supervisor_id"),
    WORKER_HEARTBEATS((short)2, "worker_heartbeats");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.storm.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // SUPERVISOR_ID
          return SUPERVISOR_ID;
        case 2: // WORKER_HEARTBEATS
          return WORKER_HEARTBEATS;
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
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.storm.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SUPERVISOR_ID, new org.apache.storm.thrift.meta_data.FieldMetaData("supervisor_id", org.apache.storm.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.WORKER_HEARTBEATS, new org.apache.storm.thrift.meta_data.FieldMetaData("worker_heartbeats", org.apache.storm.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.storm.thrift.meta_data.ListMetaData(org.apache.storm.thrift.protocol.TType.LIST, 
            new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, SupervisorWorkerHeartbeat.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.storm.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SupervisorWorkerHeartbeats.class, metaDataMap);
  }

  public SupervisorWorkerHeartbeats() {
  }

  public SupervisorWorkerHeartbeats(
    java.lang.String supervisor_id,
    java.util.List<SupervisorWorkerHeartbeat> worker_heartbeats)
  {
    this();
    this.supervisor_id = supervisor_id;
    this.worker_heartbeats = worker_heartbeats;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SupervisorWorkerHeartbeats(SupervisorWorkerHeartbeats other) {
    if (other.is_set_supervisor_id()) {
      this.supervisor_id = other.supervisor_id;
    }
    if (other.is_set_worker_heartbeats()) {
      java.util.List<SupervisorWorkerHeartbeat> __this__worker_heartbeats = new java.util.ArrayList<SupervisorWorkerHeartbeat>(other.worker_heartbeats.size());
      for (SupervisorWorkerHeartbeat other_element : other.worker_heartbeats) {
        __this__worker_heartbeats.add(new SupervisorWorkerHeartbeat(other_element));
      }
      this.worker_heartbeats = __this__worker_heartbeats;
    }
  }

  public SupervisorWorkerHeartbeats deepCopy() {
    return new SupervisorWorkerHeartbeats(this);
  }

  @Override
  public void clear() {
    this.supervisor_id = null;
    this.worker_heartbeats = null;
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.lang.String get_supervisor_id() {
    return this.supervisor_id;
  }

  public void set_supervisor_id(@org.apache.storm.thrift.annotation.Nullable java.lang.String supervisor_id) {
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

  public int get_worker_heartbeats_size() {
    return (this.worker_heartbeats == null) ? 0 : this.worker_heartbeats.size();
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.util.Iterator<SupervisorWorkerHeartbeat> get_worker_heartbeats_iterator() {
    return (this.worker_heartbeats == null) ? null : this.worker_heartbeats.iterator();
  }

  public void add_to_worker_heartbeats(SupervisorWorkerHeartbeat elem) {
    if (this.worker_heartbeats == null) {
      this.worker_heartbeats = new java.util.ArrayList<SupervisorWorkerHeartbeat>();
    }
    this.worker_heartbeats.add(elem);
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.util.List<SupervisorWorkerHeartbeat> get_worker_heartbeats() {
    return this.worker_heartbeats;
  }

  public void set_worker_heartbeats(@org.apache.storm.thrift.annotation.Nullable java.util.List<SupervisorWorkerHeartbeat> worker_heartbeats) {
    this.worker_heartbeats = worker_heartbeats;
  }

  public void unset_worker_heartbeats() {
    this.worker_heartbeats = null;
  }

  /** Returns true if field worker_heartbeats is set (has been assigned a value) and false otherwise */
  public boolean is_set_worker_heartbeats() {
    return this.worker_heartbeats != null;
  }

  public void set_worker_heartbeats_isSet(boolean value) {
    if (!value) {
      this.worker_heartbeats = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.storm.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case SUPERVISOR_ID:
      if (value == null) {
        unset_supervisor_id();
      } else {
        set_supervisor_id((java.lang.String)value);
      }
      break;

    case WORKER_HEARTBEATS:
      if (value == null) {
        unset_worker_heartbeats();
      } else {
        set_worker_heartbeats((java.util.List<SupervisorWorkerHeartbeat>)value);
      }
      break;

    }
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case SUPERVISOR_ID:
      return get_supervisor_id();

    case WORKER_HEARTBEATS:
      return get_worker_heartbeats();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case SUPERVISOR_ID:
      return is_set_supervisor_id();
    case WORKER_HEARTBEATS:
      return is_set_worker_heartbeats();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof SupervisorWorkerHeartbeats)
      return this.equals((SupervisorWorkerHeartbeats)that);
    return false;
  }

  public boolean equals(SupervisorWorkerHeartbeats that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_supervisor_id = true && this.is_set_supervisor_id();
    boolean that_present_supervisor_id = true && that.is_set_supervisor_id();
    if (this_present_supervisor_id || that_present_supervisor_id) {
      if (!(this_present_supervisor_id && that_present_supervisor_id))
        return false;
      if (!this.supervisor_id.equals(that.supervisor_id))
        return false;
    }

    boolean this_present_worker_heartbeats = true && this.is_set_worker_heartbeats();
    boolean that_present_worker_heartbeats = true && that.is_set_worker_heartbeats();
    if (this_present_worker_heartbeats || that_present_worker_heartbeats) {
      if (!(this_present_worker_heartbeats && that_present_worker_heartbeats))
        return false;
      if (!this.worker_heartbeats.equals(that.worker_heartbeats))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((is_set_supervisor_id()) ? 131071 : 524287);
    if (is_set_supervisor_id())
      hashCode = hashCode * 8191 + supervisor_id.hashCode();

    hashCode = hashCode * 8191 + ((is_set_worker_heartbeats()) ? 131071 : 524287);
    if (is_set_worker_heartbeats())
      hashCode = hashCode * 8191 + worker_heartbeats.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(SupervisorWorkerHeartbeats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(is_set_supervisor_id()).compareTo(other.is_set_supervisor_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_supervisor_id()) {
      lastComparison = org.apache.storm.thrift.TBaseHelper.compareTo(this.supervisor_id, other.supervisor_id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(is_set_worker_heartbeats()).compareTo(other.is_set_worker_heartbeats());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_worker_heartbeats()) {
      lastComparison = org.apache.storm.thrift.TBaseHelper.compareTo(this.worker_heartbeats, other.worker_heartbeats);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.storm.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.storm.thrift.protocol.TProtocol iprot) throws org.apache.storm.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.storm.thrift.protocol.TProtocol oprot) throws org.apache.storm.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("SupervisorWorkerHeartbeats(");
    boolean first = true;

    sb.append("supervisor_id:");
    if (this.supervisor_id == null) {
      sb.append("null");
    } else {
      sb.append(this.supervisor_id);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("worker_heartbeats:");
    if (this.worker_heartbeats == null) {
      sb.append("null");
    } else {
      sb.append(this.worker_heartbeats);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.storm.thrift.TException {
    // check for required fields
    if (!is_set_supervisor_id()) {
      throw new org.apache.storm.thrift.protocol.TProtocolException("Required field 'supervisor_id' is unset! Struct:" + toString());
    }

    if (!is_set_worker_heartbeats()) {
      throw new org.apache.storm.thrift.protocol.TProtocolException("Required field 'worker_heartbeats' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.storm.thrift.protocol.TCompactProtocol(new org.apache.storm.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.storm.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.storm.thrift.protocol.TCompactProtocol(new org.apache.storm.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.storm.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class SupervisorWorkerHeartbeatsStandardSchemeFactory implements org.apache.storm.thrift.scheme.SchemeFactory {
    public SupervisorWorkerHeartbeatsStandardScheme getScheme() {
      return new SupervisorWorkerHeartbeatsStandardScheme();
    }
  }

  private static class SupervisorWorkerHeartbeatsStandardScheme extends org.apache.storm.thrift.scheme.StandardScheme<SupervisorWorkerHeartbeats> {

    public void read(org.apache.storm.thrift.protocol.TProtocol iprot, SupervisorWorkerHeartbeats struct) throws org.apache.storm.thrift.TException {
      org.apache.storm.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.storm.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SUPERVISOR_ID
            if (schemeField.type == org.apache.storm.thrift.protocol.TType.STRING) {
              struct.supervisor_id = iprot.readString();
              struct.set_supervisor_id_isSet(true);
            } else { 
              org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // WORKER_HEARTBEATS
            if (schemeField.type == org.apache.storm.thrift.protocol.TType.LIST) {
              {
                org.apache.storm.thrift.protocol.TList _list878 = iprot.readListBegin();
                struct.worker_heartbeats = new java.util.ArrayList<SupervisorWorkerHeartbeat>(_list878.size);
                @org.apache.storm.thrift.annotation.Nullable SupervisorWorkerHeartbeat _elem879;
                for (int _i880 = 0; _i880 < _list878.size; ++_i880)
                {
                  _elem879 = new SupervisorWorkerHeartbeat();
                  _elem879.read(iprot);
                  struct.worker_heartbeats.add(_elem879);
                }
                iprot.readListEnd();
              }
              struct.set_worker_heartbeats_isSet(true);
            } else { 
              org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.storm.thrift.protocol.TProtocol oprot, SupervisorWorkerHeartbeats struct) throws org.apache.storm.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.supervisor_id != null) {
        oprot.writeFieldBegin(SUPERVISOR_ID_FIELD_DESC);
        oprot.writeString(struct.supervisor_id);
        oprot.writeFieldEnd();
      }
      if (struct.worker_heartbeats != null) {
        oprot.writeFieldBegin(WORKER_HEARTBEATS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.storm.thrift.protocol.TList(org.apache.storm.thrift.protocol.TType.STRUCT, struct.worker_heartbeats.size()));
          for (SupervisorWorkerHeartbeat _iter881 : struct.worker_heartbeats)
          {
            _iter881.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SupervisorWorkerHeartbeatsTupleSchemeFactory implements org.apache.storm.thrift.scheme.SchemeFactory {
    public SupervisorWorkerHeartbeatsTupleScheme getScheme() {
      return new SupervisorWorkerHeartbeatsTupleScheme();
    }
  }

  private static class SupervisorWorkerHeartbeatsTupleScheme extends org.apache.storm.thrift.scheme.TupleScheme<SupervisorWorkerHeartbeats> {

    @Override
    public void write(org.apache.storm.thrift.protocol.TProtocol prot, SupervisorWorkerHeartbeats struct) throws org.apache.storm.thrift.TException {
      org.apache.storm.thrift.protocol.TTupleProtocol oprot = (org.apache.storm.thrift.protocol.TTupleProtocol) prot;
      oprot.writeString(struct.supervisor_id);
      {
        oprot.writeI32(struct.worker_heartbeats.size());
        for (SupervisorWorkerHeartbeat _iter882 : struct.worker_heartbeats)
        {
          _iter882.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.storm.thrift.protocol.TProtocol prot, SupervisorWorkerHeartbeats struct) throws org.apache.storm.thrift.TException {
      org.apache.storm.thrift.protocol.TTupleProtocol iprot = (org.apache.storm.thrift.protocol.TTupleProtocol) prot;
      struct.supervisor_id = iprot.readString();
      struct.set_supervisor_id_isSet(true);
      {
        org.apache.storm.thrift.protocol.TList _list883 = new org.apache.storm.thrift.protocol.TList(org.apache.storm.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.worker_heartbeats = new java.util.ArrayList<SupervisorWorkerHeartbeat>(_list883.size);
        @org.apache.storm.thrift.annotation.Nullable SupervisorWorkerHeartbeat _elem884;
        for (int _i885 = 0; _i885 < _list883.size; ++_i885)
        {
          _elem884 = new SupervisorWorkerHeartbeat();
          _elem884.read(iprot);
          struct.worker_heartbeats.add(_elem884);
        }
      }
      struct.set_worker_heartbeats_isSet(true);
    }
  }

  private static <S extends org.apache.storm.thrift.scheme.IScheme> S scheme(org.apache.storm.thrift.protocol.TProtocol proto) {
    return (org.apache.storm.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

