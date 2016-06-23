/*
 * Copyright Blue Jeans Network.
 */
package com.bluejeans.kafka;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import net.jodah.typetools.TypeResolver;
import net.jodah.typetools.TypeResolver.Unknown;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Any object Deserializer
 *
 * @author Dinesh Ilindra
 * @param <T>
 *            the object type
 */
public class ObjectDeserializer<T> implements Deserializer<T> {

    private String methodName = "valueOf";
    private String encoding = "UTF8";

    private Class<?> objectClass;

    public ObjectDeserializer() {
        objectClass = TypeResolver.resolveRawArguments(ObjectDeserializer.class, getClass())[0];
        if (objectClass == Unknown.class) {
            objectClass = String.class;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.kafka.common.serialization.Deserializer#configure(java.util
     * .Map, boolean)
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // for encoding
        if (configs == null) {
            return;
        }
        final String encodingProp = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(encodingProp);
        if (encodingValue == null) {
            encodingValue = configs.get("deserializer.encoding");
        }
        if (encodingValue != null && encodingValue instanceof String) {
            encoding = (String) encodingValue;
        }
        // for method name
        final String methodProp = isKey ? "key.deserializer.methodName" : "value.deserializer.methodName";
        Object method = configs.get(methodProp);
        if (method == null) {
            method = configs.get("deserializer.methodName");
        }
        if (method != null && method instanceof String) {
            methodName = (String) method;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.kafka.common.serialization.Deserializer#deserialize(java.lang
     * .String, byte[])
     */
    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(final String topic, final byte[] data) {
        try {
            if (data == null) {
                return null;
            } else if (objectClass == String.class) {
                return (T) new String(data, encoding);
            } else {
                return (T) objectClass.getMethod(methodName, String.class).invoke(null, new String(data, encoding));
            }
        } catch (final UnsupportedEncodingException uee) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding "
                    + encoding, uee);
        } catch (final ReflectiveOperationException roe) {
            throw new SerializationException("Error when deserializing due to underlying method - " + roe.getMessage(),
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
