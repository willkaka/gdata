package com.hyw.gdata;

import com.hyw.gdata.utils.QFunction;
import com.hyw.gdata.utils.QueryUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class NUpdateWrapper<T> {

    private Connection connection;
    private String updateType = "UPDATE";
    private Map<String,Class<T>> classMap = new HashMap<>();
    private Map<String,Object> setMap = new HashMap<>();
    private List<UpdCondition> updConditionList = new ArrayList<>();
    private List<Field> classFieldList = new ArrayList<>();

    public String getSql(){
        StringBuilder sql = new StringBuilder();
        sql.append(updateType).append(" ");
        if("DELETE".equalsIgnoreCase(updateType)) sql.append("FROM ");
        int mapIndex=0;
        for(String tableName:classMap.keySet()){
            if(mapIndex>0) sql.append(",");
            sql.append(QueryUtil.toUnderlineStr(tableName));
            mapIndex++;
        }
        mapIndex=0;
        if(QueryUtil.isNotEmptyMap(setMap)){
            sql.append(" SET ");
            for(String setFieldName:setMap.keySet()){
                if(mapIndex>0) sql.append(",");
                sql.append(QueryUtil.toUnderlineStr(setFieldName)).append("=")
                .append(setMap.get(setFieldName));
                mapIndex++;
            }
        }
        if(QueryUtil.isNotEmptyList(updConditionList)) sql.append(" WHERE ");
        int condIndex=0;
        for(UpdCondition updCondition : updConditionList){
            if(condIndex>0) sql.append(" AND ");
            sql.append(QueryUtil.toUnderlineStr(updCondition.getColumn().toString()))
                    .append(" ").append(updCondition.getSymbol())
                    .append(" ").append(updCondition.getValue());
            condIndex++;
        }
//        System.out.println(sql);
        return sql.toString();
    }

    public <A,B> NUpdateWrapper<T> set(QFunction<A, B> function, Object value) {
        putTable(function);
        setMap.put(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),
                getValue(value));
        return this;
    }

    public <A,B> NUpdateWrapper<T> set(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        setMap.put(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),
                QueryUtil.toUnderlineStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NUpdateWrapper<T> eq(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"=",getValue(value));
        return this;
    }

    public <A,B> NUpdateWrapper<T> eq(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),"=",
                QueryUtil.toUnderlineStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NUpdateWrapper<T> ne(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"<>",getValue(value));
        return this;
    }

    public <A,B> NUpdateWrapper<T> ne(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),"<>",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NUpdateWrapper<T> gt(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),">",getValue(value));
        return this;
    }

    public <A,B> NUpdateWrapper<T> gt(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),">",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NUpdateWrapper<T> ge(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),">=",getValue(value));
        return this;
    }

    public <A,B> NUpdateWrapper<T> ge(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),">=",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NUpdateWrapper<T> lt(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"<",value);
        return this;
    }

    public <A,B> NUpdateWrapper<T> lt(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),"<",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NUpdateWrapper<T> le(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"<=",getValue(value));
        return this;
    }

    public <A,B> NUpdateWrapper<T> le(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),"<=",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NUpdateWrapper<T> in(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"in",getValue(value));
        return this;
    }

    public <A,B> NUpdateWrapper<T> notIn(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"not in",getValue(value));
        return this;
    }

    public <A,B> NUpdateWrapper<T> like(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"like",
                getValue("%"+String.valueOf(value)+"%"));
        return this;
    }

    public <A,B> NUpdateWrapper<T> likeRight(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"like",
                getValue(String.valueOf(value)+"%"));
        return this;
    }

    public <A,B> NUpdateWrapper<T> likeLeft(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"like",
                getValue("%"+String.valueOf(value)));
        return this;
    }

    /**
     * update by appointed key fields 按指定key更新
     * @param object object
     * @param keyFunctions appointed key fields
     * @return NUpdateWrapper
     */
    public final <A, B> NUpdateWrapper<T> updateByAptKey(T object, QFunction<A, B>... keyFunctions){
        Map<String,Field> fieldMap = QueryUtil.getObjectAllFieldMap(object.getClass());
        NUpdateWrapper<T> nUpdateWrapper = new NUpdateWrapper<T>();
        //put select conditions
        for(QFunction<A, B> function:keyFunctions){
            putTable(function);
            String fieldName = QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get",""));
            nUpdateWrapper.eq(function, getObjFieldValue(object,fieldName));
        }
        //put updated fields and values
        for(String fieldName:fieldMap.keySet()){
            nUpdateWrapper.getSetMap().put(QueryUtil.toCamelCaseStr(fieldName),getValue(getObjFieldValue(object,fieldName)));
        }
        return nUpdateWrapper;
    }

    /**
     * delete by appointed key fields 按指定key更新
     * @param object object
     * @param keyFunctions appointed key fields
     * @return NUpdateWrapper
     */
    public final <A,B> NUpdateWrapper<T> deleteByAptKey(T object, QFunction<A, B>... keyFunctions){
        NUpdateWrapper<T> nUpdateWrapper = new NUpdateWrapper<T>();
        nUpdateWrapper.setUpdateType("DELETE");
        //put select conditions
        for(QFunction<A,B> function:keyFunctions){
            putTable(function);
            String fieldName = QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get",""));
            nUpdateWrapper.eq(function, getObjFieldValue(object,fieldName));
        }
        return nUpdateWrapper;
    }

    private void putCondition(String fieldName,String symbol,Object value){
        UpdCondition updCondition = new UpdCondition();
        updCondition.setColumn(fieldName).setSymbol(symbol).setValue(value);
        updConditionList.add(updCondition);
    }

    private <A,B> void putTable(QFunction<A, B> function){
        Class clazz = QueryUtil.getClassWithFunc(function);
        String clazzName = clazz.getSimpleName();
        if(!classMap.containsKey(clazzName)){
            classMap.put(clazzName,clazz);
        }
    }
//    private <A,B> SerializedLambda putTable(QFunction<A, B> function){
//        SerializedLambda SerializedUtil = ClassUtils.resolve(function);
//        Class clazz = SerializedUtil.getImplClass();
////        Class clazz = com.baomidou.mybatisplus.core.toolkit.ClassUtils.toClassConfident(SerializedUtil.getImplClass().replace('/', '.'));
//        String clazzName = clazz.getSimpleName();
//        if(!classMap.containsKey(clazzName)){
//            classMap.put(clazzName,clazz);
//        }
//        return SerializedUtil;
//    }

    private void getClassFieldInfo(){
        classMap.forEach((tableName,clazz)->{
            classFieldList.addAll(QueryUtil.getObjectAllFieldList(clazz));
        });
    }


    private String getValue(Object value){
        if(null == value){
            return "null";
        }else if(value instanceof Integer){
            return String.valueOf(value);
        }else if(value instanceof String){
            return "'"+String.valueOf(value)+"'";
        }else if(value instanceof BigDecimal){
            BigDecimal num = (BigDecimal) value;
            return String.valueOf(value);
        }else if(value instanceof Date){
            return "'"+String.valueOf(value)+"'";
        }else if(value instanceof LocalDate){
            return "'"+String.valueOf(value)+"'";
        }else if(value instanceof LocalDateTime){
            return "'"+String.valueOf(value)+"'";
        }else{
            return "'"+String.valueOf(value)+"'";
        }
    }

    public static String getObjFieldValue(String type, Object value){
        if(null == value){
            if("int".equalsIgnoreCase(type) || "java.math.BigDecimal".equalsIgnoreCase(type)){
                return "0";
            }else{
                return "null";
            }
        }else if(value instanceof String && "null".equalsIgnoreCase((String) value)){
            return "null";
        }else if("int".equalsIgnoreCase(type)){
            return value.toString();
        }else if("boolean".equalsIgnoreCase(type)){
            return value.toString();
        }else if("char".equalsIgnoreCase(type)){
            return "'"+value.toString()+"'";
        }else if("java.math.BigDecimal".equalsIgnoreCase(type)){
            return value.toString();
        }else if("java.time.LocalDate".equalsIgnoreCase(type)){
            return "'"+value.toString()+"'";
        }else if("java.time.LocalDateTime".equalsIgnoreCase(type)){
            return "'"+value.toString()+"'";
        }else{
            return "'" + value + "'";
        }
    }


    /**
     * @Description: 获取对象 属性值
     * @param object
     * @param fieldName
     * @return
     */
    private Object getObjFieldValue(Object object, String fieldName) {
        if(object == null) {
            return null;
        }
        Class<? extends Object> class1 = object.getClass();
        Object objectValue = null;
        try {
            Field[] fields = class1.getDeclaredFields();
            for(Field field:fields){
                String objectFieldName = field.getName();
                if(objectFieldName.equalsIgnoreCase(fieldName)){
                    if (!field.isAccessible()) {
                        field.setAccessible(true);
                    }
                    objectValue = field.get(object);
                    break;
                }
            }
        }catch (Exception e) {
            log.error("对象{} 获取属性值：{} 失败", object,fieldName);
        }
        return objectValue;
    }

    public Class<T> getTableClass(){
        for(String name:classMap.keySet()){
            return classMap.get(name);
        }
        return null;
    }
}

@Setter
@Getter
@Accessors(chain = true)
class UpdCondition {
    private Object column;
    private String symbol;
    private Object value;
}
