/*
 * Copyright Blue Jeans Network.
 */
package com.bluejeans.kafka;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import net.jodah.typetools.TypeResolver;
import net.jodah.typetools.TypeResolver.Unknown;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Any object Serializer
 *
 * @author Dinesh Ilindra
 * @param <T>
 *            the object type
 */
public class ObjectSerializer<T> implements Serializer<T> {

    private String methodName = "toString";
    private String encoding = "UTF8";

    private Class<?> objectClass;

    public ObjectSerializer() {
        objectClass = TypeResolver.resolveRawArguments(ObjectSerializer.class, getClass())[0];
        if (objectClass == Unknown.class) {
            objectClass = String.class;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.kafka.common.serialization.Serializer#configure(java.util
     * .Map, boolean)
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // for encoding
        if (configs == null) {
            return;
        }
        final String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null) {
            encodingValue = configs.get("serializer.encoding");
        }
        if (encodingValue != null && encodingValue instanceof String) {
            encoding = (String) encodingValue;
        }
        // for method name
        final String methodProp = isKey ? "key.serializer.methodName" : "value.serializer.methodName";
        Object method = configs.get(methodProp);
        if (method == null) {
            method = configs.get("serializer.methodName");
        }
        if (method != null && method instanceof String) {
            methodName = (String) method;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.kafka.common.serialization.Serializer#serialize(java.lang.
     * String, java.lang.Object)
     */
    @Override
    public byte[] serialize(final String topic, final T data) {
        try {
            if (data == null) {
                return null;
            } else if (objectClass == String.class) {
                return ((String) data).getBytes(encoding);
            } else {
                return ((String) objectClass.getMethod(methodName).invoke(data)).getBytes(encoding);
            }
        } catch (final UnsupportedEncodingException uee) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding "
                    + encoding, uee);
        } catch (final ReflectiveOperationException roe) {
            throw new SerializationException("Error when serializing due to underlying method - " + roe.getMessage(),
                    roe);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.kafka.common.serialization.Deserializer#close()
     */
    @Override
    public void close() {
        // nothing to do
    }

    /**
     * @return the methodName
     */
    public String getMethodName() {
        return methodName;
    }

    /**
     * @return the encoding
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * @return the objectClass
     */
    public Class<?> getObjectClass() {
        return objectClass;
    }

}
