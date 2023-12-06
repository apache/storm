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
 * Autogenerated by Thrift Compiler (0.18.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.storm.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.18.1)")
public class HBMessageData extends org.apache.storm.thrift.TUnion<HBMessageData, HBMessageData._Fields> {
  private static final org.apache.storm.thrift.protocol.TStruct STRUCT_DESC = new org.apache.storm.thrift.protocol.TStruct("HBMessageData");
  private static final org.apache.storm.thrift.protocol.TField PATH_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("path", org.apache.storm.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.storm.thrift.protocol.TField PULSE_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("pulse", org.apache.storm.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.storm.thrift.protocol.TField BOOLVAL_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("boolval", org.apache.storm.thrift.protocol.TType.BOOL, (short)3);
  private static final org.apache.storm.thrift.protocol.TField RECORDS_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("records", org.apache.storm.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.storm.thrift.protocol.TField NODES_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("nodes", org.apache.storm.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.storm.thrift.protocol.TField MESSAGE_BLOB_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("message_blob", org.apache.storm.thrift.protocol.TType.STRING, (short)7);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.storm.thrift.TFieldIdEnum {
    PATH((short)1, "path"),
    PULSE((short)2, "pulse"),
    BOOLVAL((short)3, "boolval"),
    RECORDS((short)4, "records"),
    NODES((short)5, "nodes"),
    MESSAGE_BLOB((short)7, "message_blob");

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
        case 1: // PATH
          return PATH;
        case 2: // PULSE
          return PULSE;
        case 3: // BOOLVAL
          return BOOLVAL;
        case 4: // RECORDS
          return RECORDS;
        case 5: // NODES
          return NODES;
        case 7: // MESSAGE_BLOB
          return MESSAGE_BLOB;
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

  public static final java.util.Map<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.storm.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PATH, new org.apache.storm.thrift.meta_data.FieldMetaData("path", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PULSE, new org.apache.storm.thrift.meta_data.FieldMetaData("pulse", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, HBPulse.class)));
    tmpMap.put(_Fields.BOOLVAL, new org.apache.storm.thrift.meta_data.FieldMetaData("boolval", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.RECORDS, new org.apache.storm.thrift.meta_data.FieldMetaData("records", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, HBRecords.class)));
    tmpMap.put(_Fields.NODES, new org.apache.storm.thrift.meta_data.FieldMetaData("nodes", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, HBNodes.class)));
    tmpMap.put(_Fields.MESSAGE_BLOB, new org.apache.storm.thrift.meta_data.FieldMetaData("message_blob", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.STRING        , true)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.storm.thrift.meta_data.FieldMetaData.addStructMetaDataMap(HBMessageData.class, metaDataMap);
  }

  public HBMessageData() {
    super();
  }

  public HBMessageData(_Fields setField, java.lang.Object value) {
    super(setField, value);
  }

  public HBMessageData(HBMessageData other) {
    super(other);
  }
  @Override
  public HBMessageData deepCopy() {
    return new HBMessageData(this);
  }

  public static HBMessageData path(java.lang.String value) {
    HBMessageData x = new HBMessageData();
    x.set_path(value);
    return x;
  }

  public static HBMessageData pulse(HBPulse value) {
    HBMessageData x = new HBMessageData();
    x.set_pulse(value);
    return x;
  }

  public static HBMessageData boolval(boolean value) {
    HBMessageData x = new HBMessageData();
    x.set_boolval(value);
    return x;
  }

  public static HBMessageData records(HBRecords value) {
    HBMessageData x = new HBMessageData();
    x.set_records(value);
    return x;
  }

  public static HBMessageData nodes(HBNodes value) {
    HBMessageData x = new HBMessageData();
    x.set_nodes(value);
    return x;
  }

  public static HBMessageData message_blob(java.nio.ByteBuffer value) {
    HBMessageData x = new HBMessageData();
    x.set_message_blob(value);
    return x;
  }

  public static HBMessageData message_blob(byte[] value) {
    HBMessageData x = new HBMessageData();
    x.set_message_blob  (java.nio.ByteBuffer.wrap(value.clone()));
    return x;
  }


  @Override
  protected void checkType(_Fields setField, java.lang.Object value) throws java.lang.ClassCastException {
    switch (setField) {
      case PATH:
        if (value instanceof java.lang.String) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type java.lang.String for field 'path', but got " + value.getClass().getSimpleName());
      case PULSE:
        if (value instanceof HBPulse) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type HBPulse for field 'pulse', but got " + value.getClass().getSimpleName());
      case BOOLVAL:
        if (value instanceof java.lang.Boolean) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type java.lang.Boolean for field 'boolval', but got " + value.getClass().getSimpleName());
      case RECORDS:
        if (value instanceof HBRecords) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type HBRecords for field 'records', but got " + value.getClass().getSimpleName());
      case NODES:
        if (value instanceof HBNodes) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type HBNodes for field 'nodes', but got " + value.getClass().getSimpleName());
      case MESSAGE_BLOB:
        if (value instanceof java.nio.ByteBuffer) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type java.nio.ByteBuffer for field 'message_blob', but got " + value.getClass().getSimpleName());
      default:
        throw new java.lang.IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected java.lang.Object standardSchemeReadValue(org.apache.storm.thrift.protocol.TProtocol iprot, org.apache.storm.thrift.protocol.TField field) throws org.apache.storm.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case PATH:
          if (field.type == PATH_FIELD_DESC.type) {
            java.lang.String path;
            path = iprot.readString();
            return path;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case PULSE:
          if (field.type == PULSE_FIELD_DESC.type) {
            HBPulse pulse;
            pulse = new HBPulse();
            pulse.read(iprot);
            return pulse;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case BOOLVAL:
          if (field.type == BOOLVAL_FIELD_DESC.type) {
            java.lang.Boolean boolval;
            boolval = iprot.readBool();
            return boolval;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case RECORDS:
          if (field.type == RECORDS_FIELD_DESC.type) {
            HBRecords records;
            records = new HBRecords();
            records.read(iprot);
            return records;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case NODES:
          if (field.type == NODES_FIELD_DESC.type) {
            HBNodes nodes;
            nodes = new HBNodes();
            nodes.read(iprot);
            return nodes;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case MESSAGE_BLOB:
          if (field.type == MESSAGE_BLOB_FIELD_DESC.type) {
            java.nio.ByteBuffer message_blob;
            message_blob = iprot.readBinary();
            return message_blob;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        default:
          throw new java.lang.IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      return null;
    }
  }

  @Override
  protected void standardSchemeWriteValue(org.apache.storm.thrift.protocol.TProtocol oprot) throws org.apache.storm.thrift.TException {
    switch (setField_) {
      case PATH:
        java.lang.String path = (java.lang.String)value_;
        oprot.writeString(path);
        return;
      case PULSE:
        HBPulse pulse = (HBPulse)value_;
        pulse.write(oprot);
        return;
      case BOOLVAL:
        java.lang.Boolean boolval = (java.lang.Boolean)value_;
        oprot.writeBool(boolval);
        return;
      case RECORDS:
        HBRecords records = (HBRecords)value_;
        records.write(oprot);
        return;
      case NODES:
        HBNodes nodes = (HBNodes)value_;
        nodes.write(oprot);
        return;
      case MESSAGE_BLOB:
        java.nio.ByteBuffer message_blob = (java.nio.ByteBuffer)value_;
        oprot.writeBinary(message_blob);
        return;
      default:
        throw new java.lang.IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected java.lang.Object tupleSchemeReadValue(org.apache.storm.thrift.protocol.TProtocol iprot, short fieldID) throws org.apache.storm.thrift.TException {
    _Fields setField = _Fields.findByThriftId(fieldID);
    if (setField != null) {
      switch (setField) {
        case PATH:
          java.lang.String path;
          path = iprot.readString();
          return path;
        case PULSE:
          HBPulse pulse;
          pulse = new HBPulse();
          pulse.read(iprot);
          return pulse;
        case BOOLVAL:
          java.lang.Boolean boolval;
          boolval = iprot.readBool();
          return boolval;
        case RECORDS:
          HBRecords records;
          records = new HBRecords();
          records.read(iprot);
          return records;
        case NODES:
          HBNodes nodes;
          nodes = new HBNodes();
          nodes.read(iprot);
          return nodes;
        case MESSAGE_BLOB:
          java.nio.ByteBuffer message_blob;
          message_blob = iprot.readBinary();
          return message_blob;
        default:
          throw new java.lang.IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      throw new org.apache.storm.thrift.protocol.TProtocolException("Couldn't find a field with field id " + fieldID);
    }
  }

  @Override
  protected void tupleSchemeWriteValue(org.apache.storm.thrift.protocol.TProtocol oprot) throws org.apache.storm.thrift.TException {
    switch (setField_) {
      case PATH:
        java.lang.String path = (java.lang.String)value_;
        oprot.writeString(path);
        return;
      case PULSE:
        HBPulse pulse = (HBPulse)value_;
        pulse.write(oprot);
        return;
      case BOOLVAL:
        java.lang.Boolean boolval = (java.lang.Boolean)value_;
        oprot.writeBool(boolval);
        return;
      case RECORDS:
        HBRecords records = (HBRecords)value_;
        records.write(oprot);
        return;
      case NODES:
        HBNodes nodes = (HBNodes)value_;
        nodes.write(oprot);
        return;
      case MESSAGE_BLOB:
        java.nio.ByteBuffer message_blob = (java.nio.ByteBuffer)value_;
        oprot.writeBinary(message_blob);
        return;
      default:
        throw new java.lang.IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.storm.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case PATH:
        return PATH_FIELD_DESC;
      case PULSE:
        return PULSE_FIELD_DESC;
      case BOOLVAL:
        return BOOLVAL_FIELD_DESC;
      case RECORDS:
        return RECORDS_FIELD_DESC;
      case NODES:
        return NODES_FIELD_DESC;
      case MESSAGE_BLOB:
        return MESSAGE_BLOB_FIELD_DESC;
      default:
        throw new java.lang.IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected org.apache.storm.thrift.protocol.TStruct getStructDesc() {
    return STRUCT_DESC;
  }

  @Override
  protected _Fields enumForId(short id) {
    return _Fields.findByThriftIdOrThrow(id);
  }

  @org.apache.storm.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public java.lang.String get_path() {
    if (getSetField() == _Fields.PATH) {
      return (java.lang.String)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'path' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_path(java.lang.String value) {
    setField_ = _Fields.PATH;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.PATH");
  }

  public HBPulse get_pulse() {
    if (getSetField() == _Fields.PULSE) {
      return (HBPulse)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'pulse' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_pulse(HBPulse value) {
    setField_ = _Fields.PULSE;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.PULSE");
  }

  public boolean get_boolval() {
    if (getSetField() == _Fields.BOOLVAL) {
      return (java.lang.Boolean)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'boolval' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_boolval(boolean value) {
    setField_ = _Fields.BOOLVAL;
    value_ = value;
  }

  public HBRecords get_records() {
    if (getSetField() == _Fields.RECORDS) {
      return (HBRecords)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'records' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_records(HBRecords value) {
    setField_ = _Fields.RECORDS;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.RECORDS");
  }

  public HBNodes get_nodes() {
    if (getSetField() == _Fields.NODES) {
      return (HBNodes)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'nodes' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_nodes(HBNodes value) {
    setField_ = _Fields.NODES;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.NODES");
  }

  public byte[] get_message_blob() {
    set_message_blob(org.apache.storm.thrift.TBaseHelper.rightSize(buffer_for_message_blob()));
    java.nio.ByteBuffer b = buffer_for_message_blob();
    return b == null ? null : b.array();
  }

  public java.nio.ByteBuffer buffer_for_message_blob() {
    if (getSetField() == _Fields.MESSAGE_BLOB) {
      return org.apache.storm.thrift.TBaseHelper.copyBinary((java.nio.ByteBuffer)getFieldValue());
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'message_blob' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_message_blob(byte[] value) {
    set_message_blob  (java.nio.ByteBuffer.wrap(value.clone()));
  }

  public void set_message_blob(java.nio.ByteBuffer value) {
    setField_ = _Fields.MESSAGE_BLOB;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.MESSAGE_BLOB");
  }

  public boolean is_set_path() {
    return setField_ == _Fields.PATH;
  }


  public boolean is_set_pulse() {
    return setField_ == _Fields.PULSE;
  }


  public boolean is_set_boolval() {
    return setField_ == _Fields.BOOLVAL;
  }


  public boolean is_set_records() {
    return setField_ == _Fields.RECORDS;
  }


  public boolean is_set_nodes() {
    return setField_ == _Fields.NODES;
  }


  public boolean is_set_message_blob() {
    return setField_ == _Fields.MESSAGE_BLOB;
  }


  public boolean equals(java.lang.Object other) {
    if (other instanceof HBMessageData) {
      return equals((HBMessageData)other);
    } else {
      return false;
    }
  }

  public boolean equals(HBMessageData other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(HBMessageData other) {
    int lastComparison = org.apache.storm.thrift.TBaseHelper.compareTo(getSetField(), other.getSetField());
    if (lastComparison == 0) {
      return org.apache.storm.thrift.TBaseHelper.compareTo(getFieldValue(), other.getFieldValue());
    }
    return lastComparison;
  }


  @Override
  public int hashCode() {
    java.util.List<java.lang.Object> list = new java.util.ArrayList<java.lang.Object>();
    list.add(this.getClass().getName());
    org.apache.storm.thrift.TFieldIdEnum setField = getSetField();
    if (setField != null) {
      list.add(setField.getThriftFieldId());
      java.lang.Object value = getFieldValue();
      if (value instanceof org.apache.storm.thrift.TEnum) {
        list.add(((org.apache.storm.thrift.TEnum)getFieldValue()).getValue());
      } else {
        list.add(value);
      }
    }
    return list.hashCode();
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


}
