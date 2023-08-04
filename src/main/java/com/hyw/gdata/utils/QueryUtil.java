package com.hyw.gdata.utils;

import com.hyw.gdata.exception.DbException;
import org.springframework.lang.Nullable;

import java.io.*;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;

public class QueryUtil {

    /**
     * 判断是否为字符串
     * @param object 输入对象
     * @return 是字符串为true
     */
    public static boolean isString(Object object){
        return object instanceof String;
    }

    public static boolean isBigDecimal(Object object){
        return object instanceof BigDecimal;
    }

    public static boolean isLocalDate(Object object){
        return object instanceof LocalDate;
    }

    public static boolean isInteger(Object object){
        return object instanceof Integer;
    }

    public static boolean isDate(Object object){
        return object instanceof Date;
    }


    public static boolean isBlankStr(String s){
        return null==s || "".equals(s);
    }

    public static boolean isNotBlankStr(String s){
        return !(null==s || "".equals(s));
    }

    /***
     * 下划线命名转为驼峰命名
     * @param para 下划线命名的字符串
     */
    public static String toCamelCaseStr(String para) {
        StringBuilder result = new StringBuilder();
        String a[] = para.split("_");
        for (String s : a) {
            if (!para.contains("_")) {
                result.append(s);
                continue;
            }
            if (result.length() == 0) {
                result.append(s.toLowerCase());
            } else {
                result.append(s.substring(0, 1).toUpperCase());
                result.append(s.substring(1).toLowerCase());
            }
        }
        return result.toString();
    }

    /**
     * 功能：驼峰命名转下划线命名
     * 小写和大写紧挨一起的地方,加上分隔符,然后全部转小写
     */
    public static String toUnderlineStr(String c) {
        String separator = "_";
        c = c.replaceAll("([a-z])([A-Z])", "$1" + separator + "$2").toLowerCase();
        return c;
    }

    public static String firstCharToLowerCase(String c){
        if(isBlankStr(c)) return c;
        return c.substring(0,1).toLowerCase() + c.substring(1);
    }

    /**
     * 取对象的所有属性（包括父类属性）
     * @param clazz 对象
     * @return List<Field>
     */
    public static List<Field> getObjectAllFieldList(Class<?> clazz){
        List<Field> fieldList = new ArrayList<>();
        for (; clazz != Object.class; clazz = clazz.getSuperclass()) {//向上循环  遍历父类
            Field[] fields = clazz.getDeclaredFields();
            Collections.addAll(fieldList, fields);
        }
        return fieldList;
    }

    /**
     * 取对象的所有属性（包括父类属性）
     * @param clazz 对象
     * @return Map<String, Field>
     */
    public static Map<String, Field> getObjectAllFieldMap(Class<?> clazz){
        Map<String, Field> fieldMap = new HashMap<>();
        for (; clazz != Object.class; clazz = clazz.getSuperclass()) {//向上循环  遍历父类
            Field[] fields = clazz.getDeclaredFields();
            for(Field field:fields){
                fieldMap.put(field.getName(),field);
            }
        }
        return fieldMap;
    }

    /**
     * Return {@code true} if the supplied Collection is {@code null} or empty.
     * Otherwise, return {@code false}.
     * @param collection the Collection to check
     * @return whether the given Collection is empty
     */
    public static boolean isEmptyList(@Nullable Collection<?> collection) {
        return (collection == null || collection.isEmpty());
    }

    public static boolean isNotEmptyList(@Nullable Collection<?> collection) {
        return !(collection == null || collection.isEmpty());
    }

    /**
     * Return {@code true} if the supplied Map is {@code null} or empty.
     * Otherwise, return {@code false}.
     * @param map the Map to check
     * @return whether the given Map is empty
     */
    public static boolean isEmptyMap(@Nullable Map<?, ?> map) {
        return (map == null || map.isEmpty());
    }
    public static boolean isNotEmptyMap(@Nullable Map<?, ?> map) {
        return !(map == null || map.isEmpty());
    }

    public static <T> Class<?> getClassWithFunc(Function<T, ?> func){
        SerializedLambda SerializedUtil = QueryUtil.resolve(func);
        return SerializedUtil.getImplClass();
    }

    public static <T> Class<?> getClassWithFunc(QFunction<T, ?> func){
        SerializedLambda SerializedUtil = QueryUtil.resolve(func);
        return SerializedUtil.getImplClass();
    }

    public static <T> String getImplMethodName(QFunction<T, ?> func){
        SerializedLambda serializedUtil = QueryUtil.resolve(func);
        return serializedUtil.getImplMethodName();
    }

    public static <T> String getImplMethodName(Function<T, ?> func){
        SerializedLambda serializedUtil = QueryUtil.resolve(func);
        return serializedUtil.getImplMethodName();
    }

    /**
     * 反序列化
     * 通过反序列化转换 lambda 表达式，该方法只能序列化 lambda 表达式，不能序列化接口实现或者正常非 lambda 写法的对象
     * @param func
     * @param <T>
     * @return
     */
    public static <T> SerializedLambda resolve(Function<T, ?> func) {
        if (func == null) { return null; }
        if (!func.getClass().isSynthetic()) {
            throw new DbException("该方法仅能传入 lambda 表达式产生的合成类");
        }
        try{
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(func);
                oos.flush();
            } catch (IOException ex) {
                throw new IllegalArgumentException("Failed to serialize object of type: " + func.getClass(), ex);
            }
            byte[] bytes = baos.toByteArray();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objIn = new QueryObjectInputStream(byteArrayInputStream);
            Object obj = objIn.readObject();
            return (SerializedLambda) obj;
        }catch (Exception e){
            throw new DbException("This is impossible to happen",e);
        }
    }

    /**
     * 反序列化
     * 通过反序列化转换 lambda 表达式，该方法只能序列化 lambda 表达式，不能序列化接口实现或者正常非 lambda 写法的对象
     * @param func
     * @param <T>
     * @return
     */
    public static <T> SerializedLambda resolve(QFunction<T, ?> func) {
        if (func == null) { return null; }
        if (!func.getClass().isSynthetic()) {
            throw new DbException("该方法仅能传入 lambda 表达式产生的合成类");
        }
        try{
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(func);
                oos.flush();
            } catch (IOException ex) {
                throw new IllegalArgumentException("Failed to serialize object of type: " + func.getClass(), ex);
            }
            byte[] bytes = baos.toByteArray();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objIn = new QueryObjectInputStream(byteArrayInputStream);
            Object obj = objIn.readObject();
            return (SerializedLambda) obj;
        }catch (Exception e){
            throw new DbException("This is impossible to happen",e);
        }
    }


    /**
     * <p>
     * 请仅在确定类存在的情况下调用该方法
     * </p>
     *
     * @param name 类名称
     * @return 返回转换后的 Class
     */
    public static Class<?> toClassConfident(String name) {
        try {
            return Class.forName(name, false, getDefaultClassLoader());
        } catch (ClassNotFoundException e) {
            try {
                return Class.forName(name);
            } catch (ClassNotFoundException ex) {
                throw new DbException("找不到指定的class！请仅在明确确定会有 class 的时候，调用该方法", e);
            }
        }
    }

    /**
     * Return the default ClassLoader to use: typically the thread context
     * ClassLoader, if available; the ClassLoader that loaded the ClassUtils
     * class will be used as fallback.
     * <p>Call this method if you intend to use the thread context ClassLoader
     * in a scenario where you clearly prefer a non-null ClassLoader reference:
     * for example, for class path resource loading (but not necessarily for
     * {@code Class.forName}, which accepts a {@code null} ClassLoader
     * reference as well).
     *
     * @return the default ClassLoader (only {@code null} if even the system
     * ClassLoader isn't accessible)
     * @see Thread#getContextClassLoader()
     * @see ClassLoader#getSystemClassLoader()
     * @since 3.3.2
     */
    private static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Throwable ex) {
            // Cannot access thread context ClassLoader - falling back...
        }
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = QueryUtil.class.getClassLoader();
            if (cl == null) {
                // getClassLoader() returning null indicates the bootstrap ClassLoader
                try {
                    cl = ClassLoader.getSystemClassLoader();
                } catch (Throwable ex) {
                    // Cannot access system ClassLoader - oh well, maybe the caller can live with null...
                }
            }
        }
        return cl;
    }

    /**
     * Serialize the given object to a byte array.
     *
     * @param object the object to serialize
     * @return an array of bytes representing the object in a portable fashion
     */
    public static byte[] serialize(Object object) {
        if (object == null) {
            return null;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            oos.flush();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Failed to serialize object of type: " + object.getClass(), ex);
        }
        return baos.toByteArray();
    }

    /**
     * map转object
     * @param map 字段值map
     * @param clazz 对象类
     * @return 对象
     * @throws Exception
     */
    public static <T> T map2Object(Map<String, Object> map, Class<T> clazz) throws Exception {
        T t = clazz.newInstance();
        //map的key
        for (String o : map.keySet() ) {
            //map的key对应的属性
            Field declaredField = clazz.getDeclaredField( QueryUtil.toCamelCaseStr(o) );
            //设置可见性为true
            declaredField.setAccessible(true);
            //setter
            Object value = null;
            if(declaredField.getType().getName().equals("java.lang.String")) {
                value = map.get(o).toString();
            }else if(declaredField.getType().getName().equals("java.time.LocalDate")){
                value = LocalDate.parse(map.get(o).toString(),DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            }else if(declaredField.getType().getName().equals("java.lang.Integer")){
                value = Integer.parseInt(map.get(o).toString());
            }else if(declaredField.getType().getName().equals("java.math.BigDecimal")){
                value = new BigDecimal(map.get(o).toString());
            }
            declaredField.set(t, value);
        }
        return t;
    }

    /**
     * 取对象的所有属性（包括父类属性）
     * @param clazz 对象
     * @return List<Field>
     */
    public static List<Field> getAllFieldList(Class<?> clazz){
        List<Field> fieldList = new ArrayList<>();
        for (; clazz != Object.class; clazz = clazz.getSuperclass()) {//向上循环  遍历父类
            Field[] fields = clazz.getDeclaredFields();
            Collections.addAll(fieldList, fields);
        }
        return fieldList;
    }
}
