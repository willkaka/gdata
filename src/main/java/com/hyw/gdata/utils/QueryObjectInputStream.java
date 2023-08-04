package com.hyw.gdata.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class QueryObjectInputStream extends ObjectInputStream{

    public QueryObjectInputStream(InputStream in) throws IOException {
        super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass objectStreamClass) throws IOException, ClassNotFoundException {
        Class<?> clazz;
        try {
            clazz = QueryUtil.toClassConfident(objectStreamClass.getName());
        } catch (Exception ex) {
            clazz = super.resolveClass(objectStreamClass);
        }
        return clazz == java.lang.invoke.SerializedLambda.class ? SerializedLambda.class : clazz;
    }
}
