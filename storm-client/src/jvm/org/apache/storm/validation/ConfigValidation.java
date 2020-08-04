/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.validation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.storm.validation.ConfigValidationAnnotations.ValidatorParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides functionality for validating configuration fields.
 */
public class ConfigValidation {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigValidation.class);
    //We follow the model of service loaders (Even though it is not a service).
    private static final String CONFIG_CLASSES_NAME = "META-INF/services/" + Validated.class.getName();

    /*
     * Validator definitions
     */
    //The following come from the JVm Specification table 4.4
    // https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-4.html#jvms-4.5
    private static final int ACC_PUBLIC = 0x0001;
    private static final int ACC_STATIC = 0x0008;
    private static final int ACC_FINAL = 0x0010;
    private static final int DESIRED_FIELD_ACC = ACC_PUBLIC | ACC_STATIC | ACC_FINAL;
    private static List<Class<?>> configClasses = null;

    public static synchronized List<Class<?>> getConfigClasses() {
        if (configClasses == null) {
            List<Class<?>> ret = new ArrayList<>();
            Set<String> classesToScan = new HashSet<>();
            classesToScan.add(Config.class.getName());
            for (URL url : Utils.findResources(CONFIG_CLASSES_NAME)) {
                try {
                    try (BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()))) {
                        String line;
                        while ((line = in.readLine()) != null) {
                            line = line.replaceAll("#.*$", "").trim();
                            if (!line.isEmpty()) {
                                classesToScan.add(line);
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Error trying to read " + url, e);
                }
            }
            for (String clazz : classesToScan) {
                try {
                    ret.add(Class.forName(clazz));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            LOG.info("Will use {} for validation", ret);
            configClasses = ret;
        }
        return configClasses;
    }

    /**
     * Validates a field given field name as string uses Config.java as the default config class
     *
     * @param fieldName provided as a string
     * @param conf      map of confs
     */
    public static void validateField(String fieldName, Map<String, Object> conf) {
        validateField(fieldName, conf, getConfigClasses());
    }

    /**
     * Validates a field given field name as string.
     *
     * @param fieldName provided as a string
     * @param conf      map of confs
     * @param configs   config class
     */
    public static void validateField(String fieldName, Map<String, Object> conf, List<Class<?>> configs) {
        Field field = null;
        for (Class<?> clazz : configs) {
            try {
                field = clazz.getField(fieldName);
            } catch (NoSuchFieldException e) {
                //Ignored
            }
        }
        if (field == null) {
            throw new RuntimeException("Could not find " + fieldName + " in any of " + configs);
        }
        validateField(field, conf);
    }

    /**
     * Validates a field given field.  Calls correct ValidatorField method based on which fields are declared for the corresponding
     * annotation.
     *
     * @param field field that needs to be validated
     * @param conf  map of confs
     */
    public static void validateField(Field field, Map<String, Object> conf) {
        Annotation[] annotations = field.getAnnotations();
        if (annotations.length == 0) {
            LOG.warn("Field {} does not have validator annotation", field);
        }
        try {
            for (Annotation annotation : annotations) {
                if (annotation.annotationType().equals(Deprecated.class)) {
                    LOG.warn("{} is a deprecated config please see {}.{} for more information.",
                             field.get(null), field.getDeclaringClass(), field.getName());
                    continue;
                }
                String type = annotation.annotationType().getName();
                Class<?> validatorClass = null;
                Class<?>[] classes = ConfigValidationAnnotations.class.getDeclaredClasses();
                //check if annotation is one of our
                for (Class<?> clazz : classes) {
                    if (clazz.getName().equals(type)) {
                        validatorClass = clazz;
                        break;
                    }
                }
                if (validatorClass != null) {
                    Object v = validatorClass.cast(annotation);
                    String key = (String) field.get(null);
                    @SuppressWarnings("unchecked")
                    Class<Validator> clazz = (Class<Validator>) validatorClass
                        .getMethod(ConfigValidationAnnotations.ValidatorParams.VALIDATOR_CLASS).invoke(v);
                    Validator o = null;
                    Map<String, Object> params = getParamsFromAnnotation(validatorClass, v);
                    //two constructor signatures used to initialize validators.
                    //One constructor takes input a Map of arguments, the other doesn't take any arguments (default constructor)
                    //If validator has a constructor that takes a Map as an argument call that constructor
                    if (hasConstructor(clazz, Map.class)) {
                        o = clazz.getConstructor(Map.class).newInstance(params);
                    } else { //If not call default constructor
                        o = clazz.newInstance();
                    }
                    o.validateField(field.getName(), conf.get(key));
                }
            }
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Validate topology conf.
     * @param topoConf The topology conf.
     */
    public static void validateTopoConf(Map<String, Object> topoConf) {
        validateFields(topoConf, Arrays.asList(Config.class));
    }

    /**
     * Validate all confs in map.
     *
     * @param conf map of configs
     */
    public static void validateFields(Map<String, Object> conf) {
        validateFields(conf, getConfigClasses());
    }

    /**
     * Validate all confs in map.
     *
     * @param conf    map of configs
     * @param classes config class
     */
    public static void validateFields(Map<String, Object> conf, List<Class<?>> classes) {
        for (Class<?> clazz : classes) {
            for (Field field : clazz.getDeclaredFields()) {
                if (!isFieldAllowed(field)) {
                    continue;
                }
                Object keyObj = null;
                try {
                    keyObj = field.get(null);
                } catch (IllegalAccessException e) {
                    //This should not happen because we checked for PUBLIC in isFieldAllowed
                    throw new RuntimeException(e);
                }
                //make sure that defined key is string in case wrong stuff got put into Config.java
                if (keyObj instanceof String) {
                    String confKey = (String) keyObj;
                    if (conf.containsKey(confKey)) {
                        validateField(field, conf);
                    }
                }
            }
        }
    }

    public static boolean isFieldAllowed(Field field) {
        return field.getAnnotation(NotConf.class) == null
               && String.class.equals(field.getType())
               && ((field.getModifiers() & DESIRED_FIELD_ACC) == DESIRED_FIELD_ACC) && !field.isSynthetic();
    }

    private static Map<String, Object> getParamsFromAnnotation(Class<?> validatorClass, Object v)
        throws InvocationTargetException, IllegalAccessException {
        Map<String, Object> params = new HashMap<String, Object>();
        for (Method method : validatorClass.getDeclaredMethods()) {

            Object value = null;
            try {
                value = (Object) method.invoke(v);
            } catch (IllegalArgumentException ex) {
                value = null;
            }
            if (value != null) {
                params.put(method.getName(), value);
            }
        }
        return params;
    }

    private static boolean hasConstructor(Class<?> clazz, Class<?> paramClass) {
        Class<?>[] classes = { paramClass };
        try {
            clazz.getConstructor(classes);
        } catch (NoSuchMethodException e) {
            return false;
        }
        return true;
    }

    public abstract static class Validator {
        public Validator(Map<String, Object> params) {
        }

        public Validator() {
        }

        public abstract void validateField(String name, Object o);
    }

    /**
     * Validates if an object is not null.
     */

    public static class NotNullValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                throw new IllegalArgumentException("Field " + name + "cannot be null! Actual value: " + o);
            }
        }
    }

    /**
     * Validates basic types.
     */
    public static class SimpleTypeValidator extends Validator {

        private Class<?> type;

        public SimpleTypeValidator(Map<String, Object> params) {
            this.type = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.TYPE);
        }

        public static void validateField(String name, Class<?> type, Object o) {
            if (o == null) {
                return;
            }
            if (type.isInstance(o)) {
                return;
            }
            throw new IllegalArgumentException(
                "Field " + name + " must be of type " + type + ". Object: " + o + " actual type: " + o.getClass());
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.type, o);
        }
    }

    /**
     * Checks if the named type derives from the specified Class.
     */
    public static class DerivedTypeValidator extends Validator {

        private Class<?> baseType;

        public DerivedTypeValidator(Map<String, Object> params) {
            this.baseType = (Class<?>) params.get(ValidatorParams.BASE_TYPE);
        }

        public static void validateField(String name, Class<?> baseType, Object actualTypeName) {
            if (actualTypeName == null) {
                return;
            }
            try {
                Class<?> actualType = Class.forName(actualTypeName.toString());
                if (baseType.isAssignableFrom(actualType)) {
                    return;
                }
                throw new IllegalArgumentException(
                    "Field " + name + " must represent a type that derives from '" + baseType + "'. Specified type = " + actualTypeName);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        @Override
        public void validateField(String name, Object actualTypeName) {
            validateField(name, this.baseType, actualTypeName);
        }
    }

    public static class StringValidator extends Validator {

        private HashSet<String> acceptedValues = null;

        public StringValidator() {
        }

        public StringValidator(Map<String, Object> params) {

            this.acceptedValues =
                new HashSet<String>(Arrays.asList((String[]) params.get(ConfigValidationAnnotations.ValidatorParams.ACCEPTED_VALUES)));

            if (this.acceptedValues.isEmpty() || (this.acceptedValues.size() == 1 && this.acceptedValues.contains(""))) {
                this.acceptedValues = null;
            }
        }

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, String.class, o);
            if (this.acceptedValues != null) {
                if (!this.acceptedValues.contains((String) o)) {
                    throw new IllegalArgumentException(
                        "Field " + name + " is not an accepted value. Value: " + o + " Accepted values: " + this.acceptedValues);
                }
            }
        }
    }

    public static class BooleanValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, Boolean.class, o);
        }
    }

    public static class NumberValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, Number.class, o);
        }
    }

    public static class DoubleValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, Double.class, o);
        }
    }

    /**
     * Validates a Integer.
     */
    public static class IntegerValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            validateInteger(name, o);
        }

        public void validateInteger(String name, Object o) {
            if (o == null) {
                return;
            }
            final long i;
            if (o instanceof Number
                && (i = ((Number) o).longValue()) == ((Number) o).doubleValue()) {
                if (i <= Integer.MAX_VALUE && i >= Integer.MIN_VALUE) {
                    return;
                }
            }
            throw new IllegalArgumentException("Field " + name + " must be an Integer within type range.");
        }
    }

    /**
     * Validates an entry for ImpersonationAclUser.
     */
    public static class ImpersonationAclUserEntryValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            ConfigValidationUtils.NestableFieldValidator validator =
                ConfigValidationUtils.mapFv(ConfigValidationUtils.fv(String.class, false),
                                            ConfigValidationUtils.listFv(String.class, false), false);
            validator.validateField(name, o);
            @SuppressWarnings("unchecked")
            Map<String, List<String>> mapObject = (Map<String, List<String>>) o;
            if (!mapObject.containsKey("hosts")) {
                throw new IllegalArgumentException(name + " should contain Map entry with key: hosts");
            }
            if (!mapObject.containsKey("groups")) {
                throw new IllegalArgumentException(name + " should contain Map entry with key: groups");
            }
        }
    }

    /**
     * validates a list of has no duplicates.
     */
    public static class NoDuplicateInListValidator extends Validator {

        @Override
        public void validateField(String name, Object field) {
            if (field == null) {
                return;
            }
            //check if iterable
            SimpleTypeValidator.validateField(name, Iterable.class, field);
            HashSet<Object> objectSet = new HashSet<Object>();
            for (Object o : (Iterable<?>) field) {
                if (objectSet.contains(o)) {
                    throw new IllegalArgumentException(name + " should contain no duplicate elements. Duplicated element: " + o);
                }
                objectSet.add(o);
            }
        }
    }

    /**
     * Validates a String or a list of Strings.
     */
    public static class StringOrStringListValidator extends Validator {

        private ConfigValidationUtils.FieldValidator fv = ConfigValidationUtils.listFv(String.class, false);

        @Override
        public void validateField(String name, Object o) {

            if (o == null || o instanceof String) {
                // A null value or a String value is acceptable
                return;
            }
            this.fv.validateField(name, o);
        }
    }

    /**
     * Validates Kryo Registration.
     */
    public static class KryoRegValidator extends Validator {

        @SuppressWarnings("unchecked")
        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            if (o instanceof Iterable) {
                for (Object e : (Iterable<?>) o) {
                    if (e instanceof Map) {
                        for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) e).entrySet()) {
                            if (!(entry.getKey() instanceof String)
                                || !(entry.getValue() instanceof String)) {
                                throw new IllegalArgumentException(
                                    "Each element of the list " + name + " must be a String or a Map of Strings");
                            }
                        }
                    } else if (!(e instanceof String)) {
                        throw new IllegalArgumentException(
                            "Each element of the list " + name + " must be a String or a Map of Strings");
                    }
                }
                return;
            }
            throw new IllegalArgumentException(
                "Field " + name + " must be an Iterable containing only Strings or Maps of Strings");
        }
    }

    /*
     * Methods for validating confs
     */

    /**
     * Validates if a number is a power of 2.
     */
    public static class PowerOf2Validator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            final long i;
            if (o instanceof Number
                && (i = ((Number) o).longValue()) == ((Number) o).doubleValue()) {
                // Test whether the integer is a power of 2.
                if (i > 0 && (i & (i - 1)) == 0) {
                    return;
                }
            }
            throw new IllegalArgumentException("Field " + name + " must be a power of 2.");
        }
    }

    /**
     * Validates each entry in a list.
     */
    public static class ListEntryTypeValidator extends Validator {

        private Class<?> type;

        public ListEntryTypeValidator(Map<String, Object> params) {
            this.type = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.TYPE);
        }

        public static void validateField(String name, Class<?> type, Object o) {
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.listFv(type, false);
            validator.validateField(name, o);
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.type, o);
        }
    }

    /**
     * Validates each entry in a list against a list of custom Validators. Each validator in the list of validators must inherit or be an
     * instance of Validator class
     */
    public static class ListEntryCustomValidator extends Validator {

        private Class<?>[] entryValidators;

        public ListEntryCustomValidator(Map<String, Object> params) {
            this.entryValidators = (Class<?>[]) params.get(ConfigValidationAnnotations.ValidatorParams.ENTRY_VALIDATOR_CLASSES);
        }

        public static void validateField(String name, Class<?>[] validators, Object o)
            throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
            if (o == null) {
                return;
            }
            //check if iterable
            SimpleTypeValidator.validateField(name, Iterable.class, o);
            for (Object entry : (Iterable<?>) o) {
                for (Class<?> validator : validators) {
                    Object v = validator.getConstructor().newInstance();
                    if (v instanceof Validator) {
                        ((Validator) v).validateField(name + " list entry", entry);
                    } else {
                        LOG.warn(
                            "validator: {} cannot be used in ListEntryCustomValidator. "
                                    + "Individual entry validators must be an instance of Validator class",
                            validator.getName());
                    }
                }
            }
        }

        @Override
        public void validateField(String name, Object o) {
            try {
                validateField(name, this.entryValidators, o);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * validates each key and value in a map of a certain type.
     */
    public static class MapEntryTypeValidator extends Validator {

        private Class<?> keyType;
        private Class<?> valueType;

        public MapEntryTypeValidator(Map<String, Object> params) {
            this.keyType = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.KEY_TYPE);
            this.valueType = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.VALUE_TYPE);
        }

        public static void validateField(String name, Class<?> keyType, Class<?> valueType, Object o) {
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.mapFv(keyType, valueType, false);
            validator.validateField(name, o);
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.keyType, this.valueType, o);
        }
    }

    /**
     * validates each key and each value against the respective arrays of validators.
     */
    public static class MapEntryCustomValidator extends Validator {

        private Class<?>[] keyValidators;
        private Class<?>[] valueValidators;

        public MapEntryCustomValidator(Map<String, Object> params) {
            this.keyValidators = (Class<?>[]) params.get(ConfigValidationAnnotations.ValidatorParams.KEY_VALIDATOR_CLASSES);
            this.valueValidators = (Class<?>[]) params.get(ConfigValidationAnnotations.ValidatorParams.VALUE_VALIDATOR_CLASSES);
        }

        @SuppressWarnings("unchecked")
        public static void validateField(String name, Class<?>[] keyValidators, Class<?>[] valueValidators, Object o)
            throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
            if (o == null) {
                return;
            }
            //check if Map
            SimpleTypeValidator.validateField(name, Map.class, o);
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) o).entrySet()) {
                for (Class<?> kv : keyValidators) {
                    Object keyValidator = kv.getConstructor().newInstance();
                    if (keyValidator instanceof Validator) {
                        ((Validator) keyValidator).validateField(name + " Map key", entry.getKey());
                    } else {
                        LOG.warn(
                            "validator: {} cannot be used in MapEntryCustomValidator to validate keys. "
                                    + "Individual entry validators must be an instance of Validator class",
                            kv.getName());
                    }
                }
                for (Class<?> vv : valueValidators) {
                    Object valueValidator = vv.getConstructor().newInstance();
                    if (valueValidator instanceof Validator) {
                        ((Validator) valueValidator).validateField(name + " Map value", entry.getValue());
                    } else {
                        LOG.warn(
                            "validator: {} cannot be used in MapEntryCustomValidator to validate values. "
                                    + "Individual entry validators must be an instance of Validator class",
                            vv.getName());
                    }
                }
            }
        }

        @Override
        public void validateField(String name, Object o) {
            try {
                validateField(name, this.keyValidators, this.valueValidators, o);
            } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Validates a positive number.
     */
    public static class PositiveNumberValidator extends Validator {

        private boolean includeZero;

        public PositiveNumberValidator() {
            this.includeZero = false;
        }

        public PositiveNumberValidator(Map<String, Object> params) {
            this.includeZero = (boolean) params.get(ConfigValidationAnnotations.ValidatorParams.INCLUDE_ZERO);
        }

        public static void validateField(String name, boolean includeZero, Object o) {
            if (o == null) {
                return;
            }
            if (o instanceof Number) {
                if (includeZero) {
                    if (((Number) o).doubleValue() >= 0.0) {
                        return;
                    }
                } else {
                    if (((Number) o).doubleValue() > 0.0) {
                        return;
                    }
                }
            }
            throw new IllegalArgumentException("Field " + name + " must be a Positive Number");
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.includeZero, o);
        }
    }

    public static class ClusterMetricRegistryValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, Map.class, o);
            if (!((Map<?, ?>) o).containsKey("class")) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: class");
            }

            SimpleTypeValidator.validateField(name, String.class, ((Map<?, ?>) o).get("class"));
        }
    }

    public static class MetricRegistryValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, Map.class, o);
            if (!((Map<?, ?>) o).containsKey("class")) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: class");
            }
            if (!((Map<?, ?>) o).containsKey("parallelism.hint")) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: parallelism.hint");
            }

            SimpleTypeValidator.validateField(name, String.class, ((Map<?, ?>) o).get("class"));
            new IntegerValidator().validateField(name, ((Map<?, ?>) o).get("parallelism.hint"));
        }
    }

    public static class MetricReportersValidator extends Validator {
        private static final String CLASS = "class";
        private static final String FILTER = "filter";

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, Map.class, o);
            if (!((Map) o).containsKey(CLASS)) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: class");
            }
            if (((Map) o).containsKey(FILTER)) {
                Map filterMap = (Map) ((Map) o).get(FILTER);
                SimpleTypeValidator.validateField(CLASS, String.class, filterMap.get(CLASS));
            }
            SimpleTypeValidator.validateField(name, String.class, ((Map) o).get(CLASS));
        }
    }

    public static class EventLoggerRegistryValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, Map.class, o);
            if (!((Map<?, ?>) o).containsKey("class")) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: class");
            }

            SimpleTypeValidator.validateField(name, String.class, ((Map<?, ?>) o).get("class"));

            if (((Map<?, ?>) o).containsKey("arguments")) {
                SimpleTypeValidator.validateField(name, Map.class, ((Map<?, ?>) o).get("arguments"));
            }
        }
    }

    public static class MapOfStringToMapOfStringToObjectValidator extends Validator {
        @Override
        public void validateField(String name, Object o) {
            ConfigValidationUtils.NestableFieldValidator validator =
                ConfigValidationUtils.mapFv(ConfigValidationUtils.fv(String.class, false),
                                            ConfigValidationUtils.mapFv(String.class, Object.class, true), true);
            validator.validateField(name, o);
        }
    }

    public static class PacemakerAuthTypeValidator extends Validator {
        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                throw new IllegalArgumentException("Field " + name + " must be set.");
            }

            if (o instanceof String
                && (((String) o).equals("NONE") || ((String) o).equals("DIGEST")
                    || ((String) o).equals("KERBEROS"))) {
                return;
            }
            throw new IllegalArgumentException("Field " + name + " must be one of \"NONE\", \"DIGEST\", or \"KERBEROS\"");
        }
    }

    public static class CustomIsExactlyOneOfValidators extends Validator {
        private Class<?>[] subValidators;
        private List<String> validatorClassNames;

        public CustomIsExactlyOneOfValidators(Map<String, Object> params) {
            this.subValidators = (Class<?>[]) params.get(ConfigValidationAnnotations.ValidatorParams.VALUE_VALIDATOR_CLASSES);
            this.validatorClassNames = Arrays.asList(subValidators).stream().map(x -> x.getName()).collect(Collectors.toList());
        }

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }

            HashMap<String, Exception> validatorExceptions = new HashMap<>();
            Set<String> selectedValidators = new HashSet<>();
            for (Class<?> vv : subValidators) {
                Object valueValidator;
                try {
                    valueValidator = vv.getConstructor().newInstance();
                } catch (Exception ex) {
                    throw new IllegalArgumentException(vv.getName() + " instantiation failure", ex);
                }
                if (valueValidator instanceof Validator) {
                    try {
                        ((Validator) valueValidator).validateField(name + " " + vv.getSimpleName() + " value", o);
                        selectedValidators.add(vv.getName());
                    } catch (Exception ex) {
                        // only one will pass, so ignore all validation errors - stored for future use
                        validatorExceptions.put(vv.getName(), ex);
                    }
                } else {
                    String err = String.format("validator: %s cannot be used in CustomExactlyOneOfValidators to validate values. "
                            + "Individual entry validators must a instance of Validator class", vv.getName());
                    LOG.warn(err);
                }
            }
            // check if one and only one validation succeeded
            if (selectedValidators.isEmpty()) {
                String parseErrs = String.join(";\n\t", validatorExceptions.entrySet().stream()
                        .map(e -> String.format("%s:%s", e.getKey(), e.getValue())).collect(Collectors.toList()));
                String err = String.format("Field %s must be one of %s; parse errors are \n\t%s", name,
                        String.join(", ", validatorClassNames), parseErrs);
                throw new IllegalArgumentException(err);
            }
            if (selectedValidators.size() > 1) {
                throw new IllegalArgumentException("Field " + name + " must match exactly one of " + String.join(", ", selectedValidators));
            }
        }
    }

    public static class RasConstraintsTypeValidator extends Validator {
        public static final String CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT = "maxNodeCoLocationCnt";
        public static final String CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS = "incompatibleComponents";

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            if (!(o instanceof Map)) {
                throw new IllegalArgumentException(
                        "Field " + name + " must be an Iterable containing only Map of Maps");
            }
            Map<String, Object> map1 = (Map<String, Object>) o;
            for (Map.Entry<String, Object> entry1: map1.entrySet()) {
                String comp1 = entry1.getKey();
                Object o2 = entry1.getValue();
                if (!(o2 instanceof Map)) {
                    String err = String.format("Field %s, component %s, expecting constraints Map with keys [\"%s\", \"%s\"], in \"%s\"",
                            name, comp1, CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT, CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, o);
                    throw new IllegalArgumentException(err);
                }
                Map<String, Object> map2 = (Map<String, Object>) o2;
                for (Map.Entry<String, Object> entry2: map2.entrySet()) {
                    String constraintType = entry2.getKey();
                    Object o3 = entry2.getValue();
                    switch (constraintType) {
                        case CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT:
                            try {
                                Integer.parseInt("" + o3);
                            } catch (Exception ex) {
                                String err = String.format("Field %s, component %s, constraint %s should be a number, not \"%s\"",
                                        name, comp1, constraintType, o3);
                                throw new IllegalArgumentException(err);
                            }
                            break;

                        case CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS:
                            if (o3 instanceof String) {
                                break;
                            } else if (o3 instanceof List) {
                                for (Object otherComp : (List) o3) {
                                    if (otherComp instanceof String) {
                                        continue;
                                    }
                                    String err = String.format(
                                            "Field %s, component %s, constraintType \"%s\", expecting incompatible component-name, "
                                                    + "found instance of class \"%s\" value \"%s\"",
                                            name, comp1, constraintType, o3.getClass().getName(), otherComp);
                                    throw new IllegalArgumentException(err);
                                }
                            }
                            break;

                        default:
                            String err = String.format(
                                    "Field %s, component %s, has unsupported constraintType \"%s\", expecting one of [\"%s\", \"%s\"], "
                                            + "in \"%s\"",
                                    name, comp1, constraintType, CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT,
                                    CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, o);
                            throw new IllegalArgumentException(err);
                    }
                }
            }
        }
    }

    public static class UserResourcePoolEntryValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, Map.class, o);
            Map<?, ?> m = (Map<?, ?>) o;
            if (!m.containsKey("cpu")) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: cpu");
            }
            if (!m.containsKey("memory")) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: memory");
            }

            SimpleTypeValidator.validateField(name, Number.class, m.get("cpu"));
            SimpleTypeValidator.validateField(name, Number.class, m.get("memory"));
        }
    }

    public static class ImplementsClassValidator extends Validator {

        Class<?> classImplements;

        public ImplementsClassValidator(Map<String, Object> params) {
            this.classImplements = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.IMPLEMENTS_CLASS);
        }

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, String.class, o);
            String className = (String) o;
            try {
                Class<?> objectClass = Class.forName(className);
                if (!this.classImplements.isAssignableFrom(objectClass)) {
                    throw new IllegalArgumentException("Field " + name + " with value " + o
                                                       + " does not implement " + this.classImplements.getName());
                }
            } catch (ClassNotFoundException e) {
                //To support topologies of older version to run, we might have to loose the constraints so that
                //the configs of older version can pass the validation.
                if (className.startsWith("backtype.storm")) {
                    LOG.warn("ClassNotFoundException: {}", className);
                    LOG.warn("Replace backtype.storm with org.apache.storm and try to validate again");
                    LOG.warn("We loosen some constraints here to support topologies of older version running on the current version");
                    validateField(name, className.replace("backtype.storm", "org.apache.storm"));
                } else {
                    throw new RuntimeException("Failed to validate config " + name + " with value " + className, e);
                }
            }
        }
    }

    /**
     * Validates a list of a list of Strings.
     */
    public static class ListOfListOfStringValidator extends Validator {

        @Override
        public void validateField(String name, Object o) throws IllegalArgumentException {
            if (o == null) {
                return;
            }
            if (o instanceof List) {
                for (Object entry1 : (List) o) {
                    if (entry1 instanceof List) {
                        for (Object entry2 : (List) entry1) {
                            if (!(entry2 instanceof String)) {
                                throw new IllegalArgumentException(
                                    "Field " + name + " must be an Iterable containing only List of List of Strings");
                            }
                        }
                    } else {
                        throw new IllegalArgumentException(
                            "Field " + name + " must be an Iterable containing only List of List of Strings");
                    }
                }
            } else {
                throw new IllegalArgumentException(
                    "Field " + name + " must be an Iterable containing only List of List of Strings");
            }
        }
    }
}
