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
public class Grouping extends org.apache.storm.thrift.TUnion<Grouping, Grouping._Fields> {
  private static final org.apache.storm.thrift.protocol.TStruct STRUCT_DESC = new org.apache.storm.thrift.protocol.TStruct("Grouping");
  private static final org.apache.storm.thrift.protocol.TField FIELDS_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("fields", org.apache.storm.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.storm.thrift.protocol.TField SHUFFLE_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("shuffle", org.apache.storm.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.storm.thrift.protocol.TField ALL_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("all", org.apache.storm.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.storm.thrift.protocol.TField NONE_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("none", org.apache.storm.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.storm.thrift.protocol.TField DIRECT_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("direct", org.apache.storm.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.storm.thrift.protocol.TField CUSTOM_OBJECT_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("custom_object", org.apache.storm.thrift.protocol.TType.STRUCT, (short)6);
  private static final org.apache.storm.thrift.protocol.TField CUSTOM_SERIALIZED_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("custom_serialized", org.apache.storm.thrift.protocol.TType.STRING, (short)7);
  private static final org.apache.storm.thrift.protocol.TField LOCAL_OR_SHUFFLE_FIELD_DESC = new org.apache.storm.thrift.protocol.TField("local_or_shuffle", org.apache.storm.thrift.protocol.TType.STRUCT, (short)8);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.storm.thrift.TFieldIdEnum {
    FIELDS((short)1, "fields"),
    SHUFFLE((short)2, "shuffle"),
    ALL((short)3, "all"),
    NONE((short)4, "none"),
    DIRECT((short)5, "direct"),
    CUSTOM_OBJECT((short)6, "custom_object"),
    CUSTOM_SERIALIZED((short)7, "custom_serialized"),
    LOCAL_OR_SHUFFLE((short)8, "local_or_shuffle");

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
        case 1: // FIELDS
          return FIELDS;
        case 2: // SHUFFLE
          return SHUFFLE;
        case 3: // ALL
          return ALL;
        case 4: // NONE
          return NONE;
        case 5: // DIRECT
          return DIRECT;
        case 6: // CUSTOM_OBJECT
          return CUSTOM_OBJECT;
        case 7: // CUSTOM_SERIALIZED
          return CUSTOM_SERIALIZED;
        case 8: // LOCAL_OR_SHUFFLE
          return LOCAL_OR_SHUFFLE;
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
    tmpMap.put(_Fields.FIELDS, new org.apache.storm.thrift.meta_data.FieldMetaData("fields", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.ListMetaData(org.apache.storm.thrift.protocol.TType.LIST, 
            new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.SHUFFLE, new org.apache.storm.thrift.meta_data.FieldMetaData("shuffle", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, NullStruct.class)));
    tmpMap.put(_Fields.ALL, new org.apache.storm.thrift.meta_data.FieldMetaData("all", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, NullStruct.class)));
    tmpMap.put(_Fields.NONE, new org.apache.storm.thrift.meta_data.FieldMetaData("none", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, NullStruct.class)));
    tmpMap.put(_Fields.DIRECT, new org.apache.storm.thrift.meta_data.FieldMetaData("direct", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, NullStruct.class)));
    tmpMap.put(_Fields.CUSTOM_OBJECT, new org.apache.storm.thrift.meta_data.FieldMetaData("custom_object", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, JavaObject.class)));
    tmpMap.put(_Fields.CUSTOM_SERIALIZED, new org.apache.storm.thrift.meta_data.FieldMetaData("custom_serialized", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.FieldValueMetaData(org.apache.storm.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.LOCAL_OR_SHUFFLE, new org.apache.storm.thrift.meta_data.FieldMetaData("local_or_shuffle", org.apache.storm.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.storm.thrift.meta_data.StructMetaData(org.apache.storm.thrift.protocol.TType.STRUCT, NullStruct.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.storm.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Grouping.class, metaDataMap);
  }

  public Grouping() {
    super();
  }

  public Grouping(_Fields setField, java.lang.Object value) {
    super(setField, value);
  }

  public Grouping(Grouping other) {
    super(other);
  }
  @Override
  public Grouping deepCopy() {
    return new Grouping(this);
  }

  public static Grouping fields(java.util.List<java.lang.String> value) {
    Grouping x = new Grouping();
    x.set_fields(value);
    return x;
  }

  public static Grouping shuffle(NullStruct value) {
    Grouping x = new Grouping();
    x.set_shuffle(value);
    return x;
  }

  public static Grouping all(NullStruct value) {
    Grouping x = new Grouping();
    x.set_all(value);
    return x;
  }

  public static Grouping none(NullStruct value) {
    Grouping x = new Grouping();
    x.set_none(value);
    return x;
  }

  public static Grouping direct(NullStruct value) {
    Grouping x = new Grouping();
    x.set_direct(value);
    return x;
  }

  public static Grouping custom_object(JavaObject value) {
    Grouping x = new Grouping();
    x.set_custom_object(value);
    return x;
  }

  public static Grouping custom_serialized(java.nio.ByteBuffer value) {
    Grouping x = new Grouping();
    x.set_custom_serialized(value);
    return x;
  }

  public static Grouping custom_serialized(byte[] value) {
    Grouping x = new Grouping();
    x.set_custom_serialized  (java.nio.ByteBuffer.wrap(value.clone()));
    return x;
  }

  public static Grouping local_or_shuffle(NullStruct value) {
    Grouping x = new Grouping();
    x.set_local_or_shuffle(value);
    return x;
  }


  @Override
  protected void checkType(_Fields setField, java.lang.Object value) throws java.lang.ClassCastException {
    switch (setField) {
      case FIELDS:
        if (value instanceof java.util.List) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type java.util.List<java.lang.String> for field 'fields', but got " + value.getClass().getSimpleName());
      case SHUFFLE:
        if (value instanceof NullStruct) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type NullStruct for field 'shuffle', but got " + value.getClass().getSimpleName());
      case ALL:
        if (value instanceof NullStruct) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type NullStruct for field 'all', but got " + value.getClass().getSimpleName());
      case NONE:
        if (value instanceof NullStruct) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type NullStruct for field 'none', but got " + value.getClass().getSimpleName());
      case DIRECT:
        if (value instanceof NullStruct) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type NullStruct for field 'direct', but got " + value.getClass().getSimpleName());
      case CUSTOM_OBJECT:
        if (value instanceof JavaObject) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type JavaObject for field 'custom_object', but got " + value.getClass().getSimpleName());
      case CUSTOM_SERIALIZED:
        if (value instanceof java.nio.ByteBuffer) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type java.nio.ByteBuffer for field 'custom_serialized', but got " + value.getClass().getSimpleName());
      case LOCAL_OR_SHUFFLE:
        if (value instanceof NullStruct) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type NullStruct for field 'local_or_shuffle', but got " + value.getClass().getSimpleName());
      default:
        throw new java.lang.IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected java.lang.Object standardSchemeReadValue(org.apache.storm.thrift.protocol.TProtocol iprot, org.apache.storm.thrift.protocol.TField field) throws org.apache.storm.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case FIELDS:
          if (field.type == FIELDS_FIELD_DESC.type) {
            java.util.List<java.lang.String> fields;
            {
              org.apache.storm.thrift.protocol.TList _list8 = iprot.readListBegin();
              fields = new java.util.ArrayList<java.lang.String>(_list8.size);
              @org.apache.storm.thrift.annotation.Nullable java.lang.String _elem9;
              for (int _i10 = 0; _i10 < _list8.size; ++_i10)
              {
                _elem9 = iprot.readString();
                fields.add(_elem9);
              }
              iprot.readListEnd();
            }
            return fields;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case SHUFFLE:
          if (field.type == SHUFFLE_FIELD_DESC.type) {
            NullStruct shuffle;
            shuffle = new NullStruct();
            shuffle.read(iprot);
            return shuffle;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case ALL:
          if (field.type == ALL_FIELD_DESC.type) {
            NullStruct all;
            all = new NullStruct();
            all.read(iprot);
            return all;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case NONE:
          if (field.type == NONE_FIELD_DESC.type) {
            NullStruct none;
            none = new NullStruct();
            none.read(iprot);
            return none;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case DIRECT:
          if (field.type == DIRECT_FIELD_DESC.type) {
            NullStruct direct;
            direct = new NullStruct();
            direct.read(iprot);
            return direct;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case CUSTOM_OBJECT:
          if (field.type == CUSTOM_OBJECT_FIELD_DESC.type) {
            JavaObject custom_object;
            custom_object = new JavaObject();
            custom_object.read(iprot);
            return custom_object;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case CUSTOM_SERIALIZED:
          if (field.type == CUSTOM_SERIALIZED_FIELD_DESC.type) {
            java.nio.ByteBuffer custom_serialized;
            custom_serialized = iprot.readBinary();
            return custom_serialized;
          } else {
            org.apache.storm.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case LOCAL_OR_SHUFFLE:
          if (field.type == LOCAL_OR_SHUFFLE_FIELD_DESC.type) {
            NullStruct local_or_shuffle;
            local_or_shuffle = new NullStruct();
            local_or_shuffle.read(iprot);
            return local_or_shuffle;
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
      case FIELDS:
        java.util.List<java.lang.String> fields = (java.util.List<java.lang.String>)value_;
        {
          oprot.writeListBegin(new org.apache.storm.thrift.protocol.TList(org.apache.storm.thrift.protocol.TType.STRING, fields.size()));
          for (java.lang.String _iter11 : fields)
          {
            oprot.writeString(_iter11);
          }
          oprot.writeListEnd();
        }
        return;
      case SHUFFLE:
        NullStruct shuffle = (NullStruct)value_;
        shuffle.write(oprot);
        return;
      case ALL:
        NullStruct all = (NullStruct)value_;
        all.write(oprot);
        return;
      case NONE:
        NullStruct none = (NullStruct)value_;
        none.write(oprot);
        return;
      case DIRECT:
        NullStruct direct = (NullStruct)value_;
        direct.write(oprot);
        return;
      case CUSTOM_OBJECT:
        JavaObject custom_object = (JavaObject)value_;
        custom_object.write(oprot);
        return;
      case CUSTOM_SERIALIZED:
        java.nio.ByteBuffer custom_serialized = (java.nio.ByteBuffer)value_;
        oprot.writeBinary(custom_serialized);
        return;
      case LOCAL_OR_SHUFFLE:
        NullStruct local_or_shuffle = (NullStruct)value_;
        local_or_shuffle.write(oprot);
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
        case FIELDS:
          java.util.List<java.lang.String> fields;
          {
            org.apache.storm.thrift.protocol.TList _list12 = iprot.readListBegin();
            fields = new java.util.ArrayList<java.lang.String>(_list12.size);
            @org.apache.storm.thrift.annotation.Nullable java.lang.String _elem13;
            for (int _i14 = 0; _i14 < _list12.size; ++_i14)
            {
              _elem13 = iprot.readString();
              fields.add(_elem13);
            }
            iprot.readListEnd();
          }
          return fields;
        case SHUFFLE:
          NullStruct shuffle;
          shuffle = new NullStruct();
          shuffle.read(iprot);
          return shuffle;
        case ALL:
          NullStruct all;
          all = new NullStruct();
          all.read(iprot);
          return all;
        case NONE:
          NullStruct none;
          none = new NullStruct();
          none.read(iprot);
          return none;
        case DIRECT:
          NullStruct direct;
          direct = new NullStruct();
          direct.read(iprot);
          return direct;
        case CUSTOM_OBJECT:
          JavaObject custom_object;
          custom_object = new JavaObject();
          custom_object.read(iprot);
          return custom_object;
        case CUSTOM_SERIALIZED:
          java.nio.ByteBuffer custom_serialized;
          custom_serialized = iprot.readBinary();
          return custom_serialized;
        case LOCAL_OR_SHUFFLE:
          NullStruct local_or_shuffle;
          local_or_shuffle = new NullStruct();
          local_or_shuffle.read(iprot);
          return local_or_shuffle;
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
      case FIELDS:
        java.util.List<java.lang.String> fields = (java.util.List<java.lang.String>)value_;
        {
          oprot.writeListBegin(new org.apache.storm.thrift.protocol.TList(org.apache.storm.thrift.protocol.TType.STRING, fields.size()));
          for (java.lang.String _iter15 : fields)
          {
            oprot.writeString(_iter15);
          }
          oprot.writeListEnd();
        }
        return;
      case SHUFFLE:
        NullStruct shuffle = (NullStruct)value_;
        shuffle.write(oprot);
        return;
      case ALL:
        NullStruct all = (NullStruct)value_;
        all.write(oprot);
        return;
      case NONE:
        NullStruct none = (NullStruct)value_;
        none.write(oprot);
        return;
      case DIRECT:
        NullStruct direct = (NullStruct)value_;
        direct.write(oprot);
        return;
      case CUSTOM_OBJECT:
        JavaObject custom_object = (JavaObject)value_;
        custom_object.write(oprot);
        return;
      case CUSTOM_SERIALIZED:
        java.nio.ByteBuffer custom_serialized = (java.nio.ByteBuffer)value_;
        oprot.writeBinary(custom_serialized);
        return;
      case LOCAL_OR_SHUFFLE:
        NullStruct local_or_shuffle = (NullStruct)value_;
        local_or_shuffle.write(oprot);
        return;
      default:
        throw new java.lang.IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.storm.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case FIELDS:
        return FIELDS_FIELD_DESC;
      case SHUFFLE:
        return SHUFFLE_FIELD_DESC;
      case ALL:
        return ALL_FIELD_DESC;
      case NONE:
        return NONE_FIELD_DESC;
      case DIRECT:
        return DIRECT_FIELD_DESC;
      case CUSTOM_OBJECT:
        return CUSTOM_OBJECT_FIELD_DESC;
      case CUSTOM_SERIALIZED:
        return CUSTOM_SERIALIZED_FIELD_DESC;
      case LOCAL_OR_SHUFFLE:
        return LOCAL_OR_SHUFFLE_FIELD_DESC;
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


  public java.util.List<java.lang.String> get_fields() {
    if (getSetField() == _Fields.FIELDS) {
      return (java.util.List<java.lang.String>)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'fields' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_fields(java.util.List<java.lang.String> value) {
    setField_ = _Fields.FIELDS;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.FIELDS");
  }

  public NullStruct get_shuffle() {
    if (getSetField() == _Fields.SHUFFLE) {
      return (NullStruct)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'shuffle' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_shuffle(NullStruct value) {
    setField_ = _Fields.SHUFFLE;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.SHUFFLE");
  }

  public NullStruct get_all() {
    if (getSetField() == _Fields.ALL) {
      return (NullStruct)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'all' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_all(NullStruct value) {
    setField_ = _Fields.ALL;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.ALL");
  }

  public NullStruct get_none() {
    if (getSetField() == _Fields.NONE) {
      return (NullStruct)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'none' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_none(NullStruct value) {
    setField_ = _Fields.NONE;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.NONE");
  }

  public NullStruct get_direct() {
    if (getSetField() == _Fields.DIRECT) {
      return (NullStruct)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'direct' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_direct(NullStruct value) {
    setField_ = _Fields.DIRECT;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.DIRECT");
  }

  public JavaObject get_custom_object() {
    if (getSetField() == _Fields.CUSTOM_OBJECT) {
      return (JavaObject)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'custom_object' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_custom_object(JavaObject value) {
    setField_ = _Fields.CUSTOM_OBJECT;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.CUSTOM_OBJECT");
  }

  public byte[] get_custom_serialized() {
    set_custom_serialized(org.apache.storm.thrift.TBaseHelper.rightSize(buffer_for_custom_serialized()));
    java.nio.ByteBuffer b = buffer_for_custom_serialized();
    return b == null ? null : b.array();
  }

  public java.nio.ByteBuffer buffer_for_custom_serialized() {
    if (getSetField() == _Fields.CUSTOM_SERIALIZED) {
      return org.apache.storm.thrift.TBaseHelper.copyBinary((java.nio.ByteBuffer)getFieldValue());
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'custom_serialized' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_custom_serialized(byte[] value) {
    set_custom_serialized  (java.nio.ByteBuffer.wrap(value.clone()));
  }

  public void set_custom_serialized(java.nio.ByteBuffer value) {
    setField_ = _Fields.CUSTOM_SERIALIZED;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.CUSTOM_SERIALIZED");
  }

  public NullStruct get_local_or_shuffle() {
    if (getSetField() == _Fields.LOCAL_OR_SHUFFLE) {
      return (NullStruct)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'local_or_shuffle' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_local_or_shuffle(NullStruct value) {
    setField_ = _Fields.LOCAL_OR_SHUFFLE;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.LOCAL_OR_SHUFFLE");
  }

  public boolean is_set_fields() {
    return setField_ == _Fields.FIELDS;
  }


  public boolean is_set_shuffle() {
    return setField_ == _Fields.SHUFFLE;
  }


  public boolean is_set_all() {
    return setField_ == _Fields.ALL;
  }


  public boolean is_set_none() {
    return setField_ == _Fields.NONE;
  }


  public boolean is_set_direct() {
    return setField_ == _Fields.DIRECT;
  }


  public boolean is_set_custom_object() {
    return setField_ == _Fields.CUSTOM_OBJECT;
  }


  public boolean is_set_custom_serialized() {
    return setField_ == _Fields.CUSTOM_SERIALIZED;
  }


  public boolean is_set_local_or_shuffle() {
    return setField_ == _Fields.LOCAL_OR_SHUFFLE;
  }


  public boolean equals(java.lang.Object other) {
    if (other instanceof Grouping) {
      return equals((Grouping)other);
    } else {
      return false;
    }
  }

  public boolean equals(Grouping other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(Grouping other) {
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
