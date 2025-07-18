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
 * Autogenerated by Thrift Compiler (0.22.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.storm.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.22.0)")
public class ComponentCommon implements org.apache.storm.thrift.TBase<ComponentCommon, ComponentCommon._Fields>, java.io.Serializable, Cloneable, Comparable<ComponentCommon> {
  private static final org.apache.storm.thrift.protocol.TStruct STRUCT_DESC = new org.apache.storm.thrift.protocol.TStruct("ComponentCommon");

  private static final org.apache.storm.thrift.protocol.TField INPUTS_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("inputs", org.apache.storm.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.storm.thrift.protocol.TField STREAMS_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("streams", org.apache.storm.thrift.protocol.TType.MAP, (short)2);
  private static final org.apache.storm.thrift.protocol.TField PARALLELISM_HINT_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("parallelism_hint", org.apache.storm.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.storm.thrift.protocol.TField JSON_CONF_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("json_conf", org.apache.storm.thrift.protocol.TType.STRING, (short)4);

  private static final org.apache.storm.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ComponentCommonStandardSchemeFactory();
  private static final org.apache.storm.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ComponentCommonTupleSchemeFactory();

  private @org.apache.storm.thrift.annotation.Nullable java.util.Map<GlobalStreamId,Grouping> inputs; // required
  private @org.apache.storm.thrift.annotation.Nullable java.util.Map<java.lang.String,StreamInfo> streams; // required
  private int parallelism_hint; // optional
  private @org.apache.storm.thrift.annotation.Nullable java.lang.String json_conf; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.storm.thrift.TFieldIdEnum {
    INPUTS((short)1, "inputs"),
    STREAMS((short)2, "streams"),
    PARALLELISM_HINT((short)3, "parallelism_hint"),
    JSON_CONF((short)4, "json_conf");

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
        case 1: // INPUTS
          return INPUTS;
        case 2: // STREAMS
          return STREAMS;
        case 3: // PARALLELISM_HINT
          return PARALLELISM_HINT;
        case 4: // JSON_CONF
          return JSON_CONF;
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

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __PARALLELISM_HINT_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields[] optionals = {_Fields.PARALLELISM_HINT,_Fields.JSON_CONF};
  public static final java.util.Map<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.INPUTS, new org.apache.storm.thrift.meta_data.FieldMetaData("inputs", org.apache.storm.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.storm.thrift.meta_data.MapMetaData(org.apache.storm.thrift.protocol.TType.MAP, 
            new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, GlobalStreamId.class), 
            new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, Grouping.class))));
    tmpMap.put(_Fields.STREAMS, new org.apache.storm.thrift.meta_data.FieldMetaData("streams", org.apache.storm.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.storm.thrift.meta_data.MapMetaData(org.apache.storm.thrift.protocol.TType.MAP, 
            new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.STRING), 
            new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, StreamInfo.class))));
    tmpMap.put(_Fields.PARALLELISM_HINT, new org.apache.storm.thrift.meta_data.FieldMetaData("parallelism_hint", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.JSON_CONF, new org.apache.storm.thrift.meta_data.FieldMetaData("json_conf", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.storm.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ComponentCommon.class, metaDataMap);
  }

  public ComponentCommon() {
  }

  public ComponentCommon(
    java.util.Map<GlobalStreamId,Grouping> inputs,
    java.util.Map<java.lang.String,StreamInfo> streams)
  {
    this();
    this.inputs = inputs;
    this.streams = streams;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ComponentCommon(ComponentCommon other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.is_set_inputs()) {
      java.util.Map<GlobalStreamId,Grouping> __this__inputs = new java.util.HashMap<GlobalStreamId,Grouping>(other.inputs.size());
      for (java.util.Map.Entry<GlobalStreamId, Grouping> other_element : other.inputs.entrySet()) {

        GlobalStreamId other_element_key = other_element.getKey();
        Grouping other_element_value = other_element.getValue();

        GlobalStreamId __this__inputs_copy_key = new GlobalStreamId(other_element_key);

        Grouping __this__inputs_copy_value = new Grouping(other_element_value);

        __this__inputs.put(__this__inputs_copy_key, __this__inputs_copy_value);
      }
      this.inputs = __this__inputs;
    }
    if (other.is_set_streams()) {
      java.util.Map<java.lang.String,StreamInfo> __this__streams = new java.util.HashMap<java.lang.String,StreamInfo>(other.streams.size());
      for (java.util.Map.Entry<java.lang.String, StreamInfo> other_element : other.streams.entrySet()) {

        java.lang.String other_element_key = other_element.getKey();
        StreamInfo other_element_value = other_element.getValue();

        java.lang.String __this__streams_copy_key = other_element_key;

        StreamInfo __this__streams_copy_value = new StreamInfo(other_element_value);

        __this__streams.put(__this__streams_copy_key, __this__streams_copy_value);
      }
      this.streams = __this__streams;
    }
    this.parallelism_hint = other.parallelism_hint;
    if (other.is_set_json_conf()) {
      this.json_conf = other.json_conf;
    }
  }

  @Override
  public ComponentCommon deepCopy() {
    return new ComponentCommon(this);
  }

  @Override
  public void clear() {
    this.inputs = null;
    this.streams = null;
    set_parallelism_hint_isSet(false);
    this.parallelism_hint = 0;
    this.json_conf = null;
  }

  public int get_inputs_size() {
    return (this.inputs == null) ? 0 : this.inputs.size();
  }

  public void put_to_inputs(GlobalStreamId key, Grouping val) {
    if (this.inputs == null) {
      this.inputs = new java.util.HashMap<GlobalStreamId,Grouping>();
    }
    this.inputs.put(key, val);
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.util.Map<GlobalStreamId,Grouping> get_inputs() {
    return this.inputs;
  }

  public void set_inputs(@org.apache.storm.thrift.annotation.Nullable java.util.Map<GlobalStreamId,Grouping> inputs) {
    this.inputs = inputs;
  }

  public void unset_inputs() {
    this.inputs = null;
  }

  /** Returns true if field inputs is set (has been assigned a value) and false otherwise */
  public boolean is_set_inputs() {
    return this.inputs != null;
  }

  public void set_inputs_isSet(boolean value) {
    if (!value) {
      this.inputs = null;
    }
  }

  public int get_streams_size() {
    return (this.streams == null) ? 0 : this.streams.size();
  }

  public void put_to_streams(java.lang.String key, StreamInfo val) {
    if (this.streams == null) {
      this.streams = new java.util.HashMap<java.lang.String,StreamInfo>();
    }
    this.streams.put(key, val);
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.util.Map<java.lang.String,StreamInfo> get_streams() {
    return this.streams;
  }

  public void set_streams(@org.apache.storm.thrift.annotation.Nullable java.util.Map<java.lang.String,StreamInfo> streams) {
    this.streams = streams;
  }

  public void unset_streams() {
    this.streams = null;
  }

  /** Returns true if field streams is set (has been assigned a value) and false otherwise */
  public boolean is_set_streams() {
    return this.streams != null;
  }

  public void set_streams_isSet(boolean value) {
    if (!value) {
      this.streams = null;
    }
  }

  public int get_parallelism_hint() {
    return this.parallelism_hint;
  }

  public void set_parallelism_hint(int parallelism_hint) {
    this.parallelism_hint = parallelism_hint;
    set_parallelism_hint_isSet(true);
  }

  public void unset_parallelism_hint() {
    __isset_bitfield = org.apache.storm.thrift.EncodingUtils.clearBit(__isset_bitfield, __PARALLELISM_HINT_ISSET_ID);
  }

  /** Returns true if field parallelism_hint is set (has been assigned a value) and false otherwise */
  public boolean is_set_parallelism_hint() {
    return org.apache.storm.thrift.EncodingUtils.testBit(__isset_bitfield, __PARALLELISM_HINT_ISSET_ID);
  }

  public void set_parallelism_hint_isSet(boolean value) {
    __isset_bitfield = org.apache.storm.thrift.EncodingUtils.setBit(__isset_bitfield, __PARALLELISM_HINT_ISSET_ID, value);
  }

  @org.apache.storm.thrift.annotation.Nullable
  public java.lang.String get_json_conf() {
    return this.json_conf;
  }

  public void set_json_conf(@org.apache.storm.thrift.annotation.Nullable java.lang.String json_conf) {
    this.json_conf = json_conf;
  }

  public void unset_json_conf() {
    this.json_conf = null;
  }

  /** Returns true if field json_conf is set (has been assigned a value) and false otherwise */
  public boolean is_set_json_conf() {
    return this.json_conf != null;
  }

  public void set_json_conf_isSet(boolean value) {
    if (!value) {
      this.json_conf = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.storm.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case INPUTS:
      if (value == null) {
        unset_inputs();
      } else {
        set_inputs((java.util.Map<GlobalStreamId,Grouping>)value);
      }
      break;

    case STREAMS:
      if (value == null) {
        unset_streams();
      } else {
        set_streams((java.util.Map<java.lang.String,StreamInfo>)value);
      }
      break;

    case PARALLELISM_HINT:
      if (value == null) {
        unset_parallelism_hint();
      } else {
        set_parallelism_hint((java.lang.Integer)value);
      }
      break;

    case JSON_CONF:
      if (value == null) {
        unset_json_conf();
      } else {
        set_json_conf((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.storm.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case INPUTS:
      return get_inputs();

    case STREAMS:
      return get_streams();

    case PARALLELISM_HINT:
      return get_parallelism_hint();

    case JSON_CONF:
      return get_json_conf();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case INPUTS:
      return is_set_inputs();
    case STREAMS:
      return is_set_streams();
    case PARALLELISM_HINT:
      return is_set_parallelism_hint();
    case JSON_CONF:
      return is_set_json_conf();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof ComponentCommon)
      return this.equals((ComponentCommon)that);
    return false;
  }

  public boolean equals(ComponentCommon that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_inputs = true && this.is_set_inputs();
    boolean that_present_inputs = true && that.is_set_inputs();
    if (this_present_inputs || that_present_inputs) {
      if (!(this_present_inputs && that_present_inputs))
        return false;
      if (!this.inputs.equals(that.inputs))
        return false;
    }

    boolean this_present_streams = true && this.is_set_streams();
    boolean that_present_streams = true && that.is_set_streams();
    if (this_present_streams || that_present_streams) {
      if (!(this_present_streams && that_present_streams))
        return false;
      if (!this.streams.equals(that.streams))
        return false;
    }

    boolean this_present_parallelism_hint = true && this.is_set_parallelism_hint();
    boolean that_present_parallelism_hint = true && that.is_set_parallelism_hint();
    if (this_present_parallelism_hint || that_present_parallelism_hint) {
      if (!(this_present_parallelism_hint && that_present_parallelism_hint))
        return false;
      if (this.parallelism_hint != that.parallelism_hint)
        return false;
    }

    boolean this_present_json_conf = true && this.is_set_json_conf();
    boolean that_present_json_conf = true && that.is_set_json_conf();
    if (this_present_json_conf || that_present_json_conf) {
      if (!(this_present_json_conf && that_present_json_conf))
        return false;
      if (!this.json_conf.equals(that.json_conf))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((is_set_inputs()) ? 131071 : 524287);
    if (is_set_inputs())
      hashCode = hashCode * 8191 + inputs.hashCode();

    hashCode = hashCode * 8191 + ((is_set_streams()) ? 131071 : 524287);
    if (is_set_streams())
      hashCode = hashCode * 8191 + streams.hashCode();

    hashCode = hashCode * 8191 + ((is_set_parallelism_hint()) ? 131071 : 524287);
    if (is_set_parallelism_hint())
      hashCode = hashCode * 8191 + parallelism_hint;

    hashCode = hashCode * 8191 + ((is_set_json_conf()) ? 131071 : 524287);
    if (is_set_json_conf())
      hashCode = hashCode * 8191 + json_conf.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(ComponentCommon other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(is_set_inputs(), other.is_set_inputs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_inputs()) {
      lastComparison = org.apache.storm.thrift.TBaseHelper.compareTo(this.inputs, other.inputs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(is_set_streams(), other.is_set_streams());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_streams()) {
      lastComparison = org.apache.storm.thrift.TBaseHelper.compareTo(this.streams, other.streams);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(is_set_parallelism_hint(), other.is_set_parallelism_hint());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_parallelism_hint()) {
      lastComparison = org.apache.storm.thrift.TBaseHelper.compareTo(this.parallelism_hint, other.parallelism_hint);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(is_set_json_conf(), other.is_set_json_conf());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_json_conf()) {
      lastComparison = org.apache.storm.thrift.TBaseHelper.compareTo(this.json_conf, other.json_conf);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.storm.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.storm.thrift.protocol.TProtocol iprot) throws org.apache.storm.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.storm.thrift.protocol.TProtocol oprot) throws org.apache.storm.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ComponentCommon(");
    boolean first = true;

    sb.append("inputs:");
    if (this.inputs == null) {
      sb.append("null");
    } else {
      sb.append(this.inputs);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("streams:");
    if (this.streams == null) {
      sb.append("null");
    } else {
      sb.append(this.streams);
    }
    first = false;
    if (is_set_parallelism_hint()) {
      if (!first) sb.append(", ");
      sb.append("parallelism_hint:");
      sb.append(this.parallelism_hint);
      first = false;
    }
    if (is_set_json_conf()) {
      if (!first) sb.append(", ");
      sb.append("json_conf:");
      if (this.json_conf == null) {
        sb.append("null");
      } else {
        sb.append(this.json_conf);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.storm.thrift.TException {
    // check for required fields
    if (!is_set_inputs()) {
      throw new org.apache.storm.thrift.protocol.TProtocolException("Required field 'inputs' is unset! Struct:" + toString());
    }

    if (!is_set_streams()) {
      throw new org.apache.storm.thrift.protocol.TProtocolException("Required field 'streams' is unset! Struct:" + toString());
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.storm.thrift.protocol.TCompactProtocol(new org.apache.storm.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.storm.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ComponentCommonStandardSchemeFactory implements org.apache.storm.thrift.scheme.SchemeFactory {
    @Override
    public ComponentCommonStandardScheme getScheme() {
      return new ComponentCommonStandardScheme();
    }
  }

  private static class ComponentCommonStandardScheme extends org.apache.storm.thrift.scheme.StandardScheme<ComponentCommon> {

    @Override
    public void read(org.apache.storm.thrift.protocol.TProtocol iprot, ComponentCommon struct) throws org.apache.storm.thrift.TException {
      org.apache.storm.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.storm.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // INPUTS
            if (schemeField.type == org.apache.storm.thrift.protocol.TType.MAP) {
              {
                org.apache.storm.thrift.protocol.TMap _map24 = iprot.readMapBegin();
                struct.inputs = new java.util.HashMap<GlobalStreamId,Grouping>(2*_map24.size);
                @org.apache.storm.thrift.annotation.Nullable GlobalStreamId _key25;
                @org.apache.storm.thrift.annotation.Nullable Grouping _val26;
                for (int _i27 = 0; _i27 < _map24.size; ++_i27)
                {
                  _key25 = new GlobalStreamId();
                  _key25.read(iprot);
                  _val26 = new Grouping();
                  _val26.read(iprot);
                  struct.inputs.put(_key25, _val26);
                }
                iprot.readMapEnd();
              }
              struct.set_inputs_isSet(true);
            } else { 
              org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STREAMS
            if (schemeField.type == org.apache.storm.thrift.protocol.TType.MAP) {
              {
                org.apache.storm.thrift.protocol.TMap _map28 = iprot.readMapBegin();
                struct.streams = new java.util.HashMap<java.lang.String,StreamInfo>(2*_map28.size);
                @org.apache.storm.thrift.annotation.Nullable java.lang.String _key29;
                @org.apache.storm.thrift.annotation.Nullable StreamInfo _val30;
                for (int _i31 = 0; _i31 < _map28.size; ++_i31)
                {
                  _key29 = iprot.readString();
                  _val30 = new StreamInfo();
                  _val30.read(iprot);
                  struct.streams.put(_key29, _val30);
                }
                iprot.readMapEnd();
              }
              struct.set_streams_isSet(true);
            } else { 
              org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // PARALLELISM_HINT
            if (schemeField.type == org.apache.storm.thrift.protocol.TType.I32) {
              struct.parallelism_hint = iprot.readI32();
              struct.set_parallelism_hint_isSet(true);
            } else { 
              org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // JSON_CONF
            if (schemeField.type == org.apache.storm.thrift.protocol.TType.STRING) {
              struct.json_conf = iprot.readString();
              struct.set_json_conf_isSet(true);
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

    @Override
    public void write(org.apache.storm.thrift.protocol.TProtocol oprot, ComponentCommon struct) throws org.apache.storm.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.inputs != null) {
        oprot.writeFieldBegin(INPUTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.storm.thrift.protocol.TMap(org.apache.storm.thrift.protocol.TType.STRUCT, org.apache.storm.thrift.protocol.TType.STRUCT, struct.inputs.size()));
          for (java.util.Map.Entry<GlobalStreamId, Grouping> _iter32 : struct.inputs.entrySet())
          {
            _iter32.getKey().write(oprot);
            _iter32.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.streams != null) {
        oprot.writeFieldBegin(STREAMS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.storm.thrift.protocol.TMap(org.apache.storm.thrift.protocol.TType.STRING, org.apache.storm.thrift.protocol.TType.STRUCT, struct.streams.size()));
          for (java.util.Map.Entry<java.lang.String, StreamInfo> _iter33 : struct.streams.entrySet())
          {
            oprot.writeString(_iter33.getKey());
            _iter33.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.is_set_parallelism_hint()) {
        oprot.writeFieldBegin(PARALLELISM_HINT_FIELD_DESC);
        oprot.writeI32(struct.parallelism_hint);
        oprot.writeFieldEnd();
      }
      if (struct.json_conf != null) {
        if (struct.is_set_json_conf()) {
          oprot.writeFieldBegin(JSON_CONF_FIELD_DESC);
          oprot.writeString(struct.json_conf);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ComponentCommonTupleSchemeFactory implements org.apache.storm.thrift.scheme.SchemeFactory {
    @Override
    public ComponentCommonTupleScheme getScheme() {
      return new ComponentCommonTupleScheme();
    }
  }

  private static class ComponentCommonTupleScheme extends org.apache.storm.thrift.scheme.TupleScheme<ComponentCommon> {

    @Override
    public void write(org.apache.storm.thrift.protocol.TProtocol prot, ComponentCommon struct) throws org.apache.storm.thrift.TException {
      org.apache.storm.thrift.protocol.TTupleProtocol oprot = (org.apache.storm.thrift.protocol.TTupleProtocol) prot;
      {
        oprot.writeI32(struct.inputs.size());
        for (java.util.Map.Entry<GlobalStreamId, Grouping> _iter34 : struct.inputs.entrySet())
        {
          _iter34.getKey().write(oprot);
          _iter34.getValue().write(oprot);
        }
      }
      {
        oprot.writeI32(struct.streams.size());
        for (java.util.Map.Entry<java.lang.String, StreamInfo> _iter35 : struct.streams.entrySet())
        {
          oprot.writeString(_iter35.getKey());
          _iter35.getValue().write(oprot);
        }
      }
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.is_set_parallelism_hint()) {
        optionals.set(0);
      }
      if (struct.is_set_json_conf()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.is_set_parallelism_hint()) {
        oprot.writeI32(struct.parallelism_hint);
      }
      if (struct.is_set_json_conf()) {
        oprot.writeString(struct.json_conf);
      }
    }

    @Override
    public void read(org.apache.storm.thrift.protocol.TProtocol prot, ComponentCommon struct) throws org.apache.storm.thrift.TException {
      org.apache.storm.thrift.protocol.TTupleProtocol iprot = (org.apache.storm.thrift.protocol.TTupleProtocol) prot;
      {
        org.apache.storm.thrift.protocol.TMap _map36 = iprot.readMapBegin(org.apache.storm.thrift.protocol.TType.STRUCT, org.apache.storm.thrift.protocol.TType.STRUCT); 
        struct.inputs = new java.util.HashMap<GlobalStreamId,Grouping>(2*_map36.size);
        @org.apache.storm.thrift.annotation.Nullable GlobalStreamId _key37;
        @org.apache.storm.thrift.annotation.Nullable Grouping _val38;
        for (int _i39 = 0; _i39 < _map36.size; ++_i39)
        {
          _key37 = new GlobalStreamId();
          _key37.read(iprot);
          _val38 = new Grouping();
          _val38.read(iprot);
          struct.inputs.put(_key37, _val38);
        }
      }
      struct.set_inputs_isSet(true);
      {
        org.apache.storm.thrift.protocol.TMap _map40 = iprot.readMapBegin(org.apache.storm.thrift.protocol.TType.STRING, org.apache.storm.thrift.protocol.TType.STRUCT); 
        struct.streams = new java.util.HashMap<java.lang.String,StreamInfo>(2*_map40.size);
        @org.apache.storm.thrift.annotation.Nullable java.lang.String _key41;
        @org.apache.storm.thrift.annotation.Nullable StreamInfo _val42;
        for (int _i43 = 0; _i43 < _map40.size; ++_i43)
        {
          _key41 = iprot.readString();
          _val42 = new StreamInfo();
          _val42.read(iprot);
          struct.streams.put(_key41, _val42);
        }
      }
      struct.set_streams_isSet(true);
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.parallelism_hint = iprot.readI32();
        struct.set_parallelism_hint_isSet(true);
      }
      if (incoming.get(1)) {
        struct.json_conf = iprot.readString();
        struct.set_json_conf_isSet(true);
      }
    }
  }

  private static <S extends org.apache.storm.thrift.scheme.IScheme> S scheme(org.apache.storm.thrift.protocol.TProtocol proto) {
    return (org.apache.storm.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

