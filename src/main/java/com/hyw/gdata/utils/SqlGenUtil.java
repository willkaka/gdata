package com.hyw.gdata.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hyw.gdata.dto.FieldAttr;
import com.hyw.gdata.dto.TableFieldInfo;
import com.hyw.gdata.exception.DbException;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlGenUtil {


    /**
     * 生成单笔记录的Insert语句
     * @param object 对象
     * @return sql
     */
    public static String getInsertSql(Object object){
        if(null == object) return null;
        List<Field> fieldList = QueryUtil.getAllFieldList(object.getClass());
        StringBuilder sql = new StringBuilder();
        String tableName = QueryUtil.toUnderlineStr(object.getClass().getSimpleName());
        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(object);

        sql.append("INSERT INTO ").append(tableName).append(" (");
        StringBuilder sqlFields = new StringBuilder();
        StringBuilder sqlValues = new StringBuilder();
        for(Field field:fieldList){
            String fieldName = QueryUtil.toUnderlineStr(field.getName());
            String fieldType = field.getType().getTypeName();
            //拼接字段名称
            if(QueryUtil.isNotBlankStr(sqlFields.toString())) sqlFields.append(",");
            sqlFields.append(fieldName);
            //拼接字段值
            if(QueryUtil.isNotBlankStr(sqlValues.toString())) sqlValues.append(",");
            sqlValues.append(getFieldValue(fieldType,jsonObject.get(field.getName())));
        }
        sql.append(sqlFields).append(") VALUES(").append(sqlValues).append(")");
        return sql.toString();
    }

    /**
     * 生成多笔记录的Insert语句
     * @param objectList 对象list
     * @param <T> 对象
     * @return sql
     */
    public static <T> String getInsertSql(List<T> objectList){
        if(QueryUtil.isEmptyList(objectList)) return null;
        List<Field> fieldList = QueryUtil.getAllFieldList(objectList.get(0).getClass());
        StringBuilder sql = new StringBuilder();
        String tableName = QueryUtil.toUnderlineStr(objectList.get(0).getClass().getSimpleName());

        sql.append("INSERT INTO ").append(tableName).append(" (");
        StringBuilder sqlFields = new StringBuilder();
        for(Field field:fieldList){
            String fieldName = field.getName();
//            String fieldType = field.getType().getTypeName();
            //拼接字段名称
            if(QueryUtil.isNotBlankStr(sqlFields.toString())) sqlFields.append(",");
            sqlFields.append(fieldName);
        }

        StringBuilder sqlValues = new StringBuilder();
        for(Object object:objectList){
            JSONObject jsonObject = (JSONObject) JSONObject.toJSON(object);
            int index=0;
            for(Field field:fieldList) {
                if(index==0) sqlValues.append(QueryUtil.isBlankStr(sqlValues.toString())?"(":",(");
                String fieldName = field.getName();
                String fieldType = field.getType().getTypeName();
                //拼接字段值
                if(index >0) sqlValues.append(",");
                sqlValues.append(getFieldValue(fieldType, jsonObject.get(fieldName)));
                index++;
            }
            sqlValues.append(")");
        }

        sql.append(sqlFields).append(") VALUES").append(sqlValues);
        return sql.toString();
    }

    /**
     * 生成单笔记录的Insert语句
     * @param tableName 对象
     * @param fieldList 对象
     * @param valueMap 对象
     * @return sql
     */
    public static String getInsertSql(String tableName,List<TableFieldInfo> fieldList,Map<String,String> valueMap){
        StringBuilder sql = new StringBuilder();

        sql.append("INSERT INTO ").append(tableName).append(" (");
        StringBuilder sqlFields = new StringBuilder();
        StringBuilder sqlValues = new StringBuilder();
        for(TableFieldInfo field:fieldList){
            String fieldName = QueryUtil.toUnderlineStr(field.getFieldName());
            String fieldType = field.getFieldType();
            //拼接字段名称
            if(QueryUtil.isNotBlankStr(sqlFields.toString())) sqlFields.append(",");
            sqlFields.append(fieldName);
            //拼接字段值
            if(QueryUtil.isNotBlankStr(sqlValues.toString())) sqlValues.append(",");
            sqlValues.append(getFieldValue(fieldType,valueMap.get(fieldName)));
        }
        sql.append(sqlFields).append(") VALUES(").append(sqlValues).append(")");
        return sql.toString();
    }


    public static String getFieldValue(String type,Object value){
        if(null == value){
            if("int".equalsIgnoreCase(type) ||
               "java.math.BigDecimal".equalsIgnoreCase(type)){
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
     * 拼接单表新增单记录sql语句
     * @param tableName 表名
     * @param fieldAttrMap 字段信息map 包含value值
     * @return sql字符串
     */
    public static String getInsertSql(Connection connection,String libName, String tableName, Map<String, FieldAttr> fieldAttrMap){
        if(QueryUtil.isBlankStr(tableName)) return null;
        if(CollectionUtils.isEmpty(fieldAttrMap)) return null;

        List<String> keyFields = DbUtil.getTablePrimaryKeys(connection,libName,tableName);
        DbException.trueThrow(QueryUtil.isEmptyList(keyFields),"数据表"+tableName+",无主键,无法更新!");

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (");
        StringBuilder sqlFields = new StringBuilder();
        StringBuilder sqlValues = new StringBuilder();
        for(String fieldName:fieldAttrMap.keySet()){
            FieldAttr fieldAttr = fieldAttrMap.get(fieldName);
            if(null==fieldAttr) continue;
            if(QueryUtil.isNotEmptyList(keyFields) &&
               keyFields.contains(fieldName) &&
               !"YES".equals(fieldAttrMap.get(fieldName).getIsAutoincrement()) &&
               (QueryUtil.isBlankStr((String)fieldAttr.getValue()))){
                throw new DbException("数据表"+tableName+",主键("+fieldName+")写入时不允许为空!");
            }
            //自动递增的主键，没有赋值时，写入时不指定数值，由数据库自动赋值。
            if(QueryUtil.isNotEmptyList(keyFields) &&
                    keyFields.contains(fieldName) &&
                    "YES".equals(fieldAttrMap.get(fieldName).getIsAutoincrement()) &&
                    (QueryUtil.isBlankStr((String)fieldAttr.getValue()))){
                continue;
            }

            if(QueryUtil.isNotBlankStr(sqlFields.toString())) sqlFields.append(",");
            sqlFields.append(fieldName);
            if(QueryUtil.isNotBlankStr(sqlValues.toString())) sqlValues.append(",");

            if(null == fieldAttr.getValue()){
                sqlValues.append("null");
            }else if("DECIMAL".equals(fieldAttr.getTypeName()) || "INT".equals(fieldAttr.getTypeName())){
                if(QueryUtil.isBlankStr((String)fieldAttr.getValue())){
                    sqlValues.append(0);
                }else {
                    sqlValues.append(fieldAttr.getValue());
                }
            }else if("DATE".equals(fieldAttr.getTypeName()) || "DATETIME".equals(fieldAttr.getTypeName()) ||
                     "VARCHAR".equals(fieldAttr.getTypeName()) || "CHAR".equals(fieldAttr.getTypeName())) {
                if(QueryUtil.isBlankStr((String)fieldAttr.getValue())){
                    sqlValues.append("null");
                }else {
                    sqlValues.append("'").append(fieldAttr.getValue()).append("' ");
                }
            }else{
                sqlValues.append("'").append(fieldAttr.getValue()).append("' ");
            }
        }
        sql.append(sqlFields).append(") VALUES(").append(sqlValues).append(")");
        return sql.toString();
    }

    /**
     * 拼接单表单记录更新sql语句
     * @param tableName 表名
     * @param fieldAttrMap 字段及值map
     * @return sql字符串
     */
    public static String getUpdateSql(Connection connection, String libName,String tableName, Map<String, FieldAttr> fieldAttrMap){
        StringBuilder sql = new StringBuilder();

        if(QueryUtil.isBlankStr(tableName)) return null;
        if(CollectionUtils.isEmpty(fieldAttrMap)) return null;

        sql.append("UPDATE").append(" ").append(tableName).append(" ").append("SET").append(" ");
        int setFieldCount = 0;
        for(String fieldName:fieldAttrMap.keySet()){
            FieldAttr fieldAttr = fieldAttrMap.get(fieldName);
            if(null==fieldAttr.getValue() && null==fieldAttr.getCurValue() ||
                    null!=fieldAttr.getValue() && fieldAttr.getValue().equals(fieldAttr.getCurValue())){
            }else{
                setFieldCount ++;
                if(setFieldCount != 1) {
                    sql.append(", ");
                }
                if("null".equals(fieldAttr.getCurValue()) ||
                   "INTEGER".equals(fieldAttr.getTypeName()) ||
                   "DECIMAL".equals(fieldAttr.getTypeName()) ||
                   "INT".equals(fieldAttr.getTypeName())){
                    sql.append(fieldName).append("=").append(fieldAttr.getCurValue());
                }else {
                    sql.append(fieldName).append("=").append("'").append(fieldAttr.getCurValue()).append("'");
                }
            }
        }
        List<String> keyFields = DbUtil.getTablePrimaryKeys(connection,libName,tableName);
        DbException.trueThrow(QueryUtil.isEmptyList(keyFields),"数据表"+tableName+",无主键,无法更新!");

        sql.append(" WHERE ");
        int keyFieldCount = 0;
        for(String keyField:keyFields){
            keyFieldCount++;
            if(keyFieldCount != 1) {
                sql.append(" AND ");
            }
            FieldAttr fieldAttr = fieldAttrMap.get(keyField);
            if("null".equals(fieldAttr.getCurValue()) ||
               "INTEGER".equals(fieldAttr.getTypeName()) ||
               "DECIMAL".equals(fieldAttr.getTypeName()) ||
               "INT".equals(fieldAttr.getTypeName())){
                sql.append(keyField).append("=").append(fieldAttr.getValue());//取原值
            }else {
                sql.append(keyField).append("=").append("'").append(fieldAttr.getValue()).append("'");//取原值
            }
        }
        return sql.toString();
    }

    /**
     * 拼接单表单记录更新sql语句
     * @param object 表名
     * @param keyFieldName 字段及值map
     * @return sql字符串
     */
    public static <T> String getUpdateSql(T object,String keyFieldName){
        if(object==null) return null;
        List<Field> fieldList = QueryUtil.getAllFieldList(object.getClass());
        String tableName = QueryUtil.toUnderlineStr(object.getClass().getSimpleName());

        StringBuilder sql = new StringBuilder().append("UPDATE ").append(tableName).append(" SET ");
        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(object);
        int index=0;
        Field keyField = null;
        for(Field field:fieldList) {
            String fieldName = field.getName();
            String fieldType = field.getType().getTypeName();
            if(fieldName.equalsIgnoreCase(keyFieldName)) {
                keyField = field;
            }
            if(index>0) sql.append(QueryUtil.isBlankStr(sql.toString())?"":", ");
            //拼接字段值
            sql.append(QueryUtil.toUnderlineStr(fieldName)).append("=")
                    .append(getFieldValue(fieldType, jsonObject.get(fieldName)));
            index++;
        }
        if(null==keyField) return null;
        sql.append(" WHERE ").append(QueryUtil.toUnderlineStr(keyFieldName)).append("=")
                .append(getFieldValue(keyField.getType().getTypeName(),
                        jsonObject.get(QueryUtil.firstCharToLowerCase(keyFieldName))));
        return sql.toString();
    }

    /**
     * 拼接单表单记录更新sql语句
     * @param objectList 表名
     * @param keyFieldName 字段及值map
     * @return sql字符串
     */
    public static <T> List<String> getUpdateSql(List<T> objectList,String keyFieldName){
        if(QueryUtil.isEmptyList(objectList)) return null;
        List<String> sqlList = new ArrayList<>();
        List<Field> fieldList = QueryUtil.getAllFieldList(objectList.get(0).getClass());
        String tableName = QueryUtil.toUnderlineStr(objectList.get(0).getClass().getSimpleName());

        for(Object object:objectList){
            StringBuilder sql = new StringBuilder().append("UPDATE ").append(tableName).append(" SET ");
            JSONObject jsonObject = (JSONObject) JSONObject.toJSON(object);
            int index=0;
            Field keyField = null;
            for(Field field:fieldList) {
                String fieldName = field.getName();
                String fieldType = field.getType().getTypeName();
                if(fieldName.equalsIgnoreCase(keyFieldName)) {
                    keyField = field;
                }
                if(index>0) sql.append(QueryUtil.isBlankStr(sql.toString())?"":", ");
                //拼接字段值
                sql.append(fieldName).append("=").append(getFieldValue(fieldType, jsonObject.get(fieldName)));
                index++;
            }
            if(null==keyField) return null;
            sql.append(" WHERE ").append(keyFieldName).append("=")
                    .append(getFieldValue(keyField.getType().getTypeName(), jsonObject.get(keyFieldName)));
            sqlList.add(sql.toString());
        }
        return sqlList;
    }

    /**
     * 拼接单表单记录更新sql语句
     * @param object 表名
     * @param keyFieldName 字段及值map
     * @return sql字符串
     */
    public static <T> String getDeleteSql(T object,String keyFieldName){
        if(object==null) return null;
        List<Field> fieldList = QueryUtil.getAllFieldList(object.getClass());
        String tableName = QueryUtil.toUnderlineStr(object.getClass().getSimpleName());

        StringBuilder sql = new StringBuilder().append("DELETE FROM ").append(tableName).append(" WHERE ");
        JSONObject jsonObject = (JSONObject) JSON.toJSON(object);
        int index=0;
        Field keyField = null;
        for(Field field:fieldList) {
            String fieldName = field.getName();
            String fieldType = field.getType().getTypeName();
            if(fieldName.equalsIgnoreCase(keyFieldName)) {
                keyField = field;
                //拼接字段值
                sql.append(QueryUtil.toUnderlineStr(fieldName)).append("=").append(getFieldValue(fieldType, jsonObject.get(fieldName)));
            }
            index++;
        }
        if(null==keyField) return null;
        return sql.toString();
    }

    /**
     * 拼接单表单记录更新sql语句
     * @param objectList 表名
     * @param keyFieldName 字段及值map
     * @return sql字符串
     */
    public static <T> List<String> getDeleteSql(List<T> objectList,String keyFieldName){
        if(QueryUtil.isEmptyList(objectList)) return null;
        List<String> sqlList = new ArrayList<>();
        List<Field> fieldList = QueryUtil.getAllFieldList(objectList.get(0).getClass());
        String tableName = QueryUtil.toUnderlineStr(objectList.get(0).getClass().getSimpleName());

        for(Object object:objectList){
            StringBuilder sql = new StringBuilder().append("DELETE FROM ").append(tableName).append(" WHERE ");
            JSONObject jsonObject = (JSONObject) JSONObject.toJSON(object);
            int index=0;
            Field keyField = null;
            for(Field field:fieldList) {
                String fieldName = field.getName();
                String fieldType = field.getType().getTypeName();
                if(fieldName.equalsIgnoreCase(keyFieldName)) {
                    keyField = field;
                    //拼接字段值
                    sql.append(fieldName).append("=").append(getFieldValue(fieldType, jsonObject.get(fieldName)));
                }
                index++;
            }
            if(null==keyField) return null;
            sqlList.add(sql.toString());
        }
        return sqlList;
    }

    /**
     * 拼接单表记录删除sql语句
     * @param tableName 表名
     * @param whereFieldValueMap 条件map
     * @return sql字符串
     */
    public static String getDeleteSql(String tableName, Map<String,Object> whereFieldValueMap){
        StringBuilder setFieldsString = new StringBuilder();
        StringBuilder whereConditionString = new StringBuilder();

        if(QueryUtil.isBlankStr(tableName)) return null;

        //where conditions
        if(!CollectionUtils.isEmpty(whereFieldValueMap)){
            for (String field : whereFieldValueMap.keySet()) {
                if(whereConditionString.length()>0)
                    whereConditionString.append(" AND ");
                whereConditionString.append(field).append("=");

                if(QueryUtil.isString(whereFieldValueMap.get(field)))
                    whereConditionString.append("'").append(whereFieldValueMap.get(field)).append("'");
                if(QueryUtil.isInteger(whereFieldValueMap.get(field)))
                    whereConditionString.append(whereFieldValueMap.get(field));
                if(QueryUtil.isBigDecimal(whereFieldValueMap.get(field)))
                    whereConditionString.append(whereFieldValueMap.get(field));
            }
        }

        return whereConditionString.length()>0?
                "DELETE FROM " + tableName + " WHERE (" + whereConditionString + ")":
                "DELETE FROM " + tableName;
    }

    /**
     *
     * @param object
     * @return
     * @throws IllegalAccessException
     */
    public static String getFieldValueExpress(Object object) throws IllegalAccessException {
        StringBuffer s = new StringBuffer();
        List<Field> fields = QueryUtil.getAllFieldList(object.getClass());

        boolean addAnd = false;
        for(Field field:fields){
            //不处理静态变量和常量
            if ( Modifier.isStatic( field.getModifiers() ) || Modifier.isFinal( field.getModifiers() ) ){
                continue;
            }
            String express = getFieldValueExpress(field,object);
            if(addAnd && QueryUtil.isNotBlankStr(express)) {
                s.append(" AND ");
            }else if(QueryUtil.isNotBlankStr(express)){
                addAnd = true;
            }
            s.append(express);
        }
        return s.toString();
    }

    /**
     * 取对象属性值并转为字符串（用于sql_where拼接）
     *   字符串加单引号
     * @param field 字段
     * @param object 对象
     * @return 属性值字符串
     */
    public static String getFieldValueExpress(Field field,Object object){
        if(null == field){
            //如果数据表定义字段比对象类字段多，则多出来的不赋值，不抛错
            return null;
        }
        Object value = null;
        field.setAccessible(true);//设置可见性为true
        try {
            value = field.get(object);
        }catch (Exception e){
            e.printStackTrace();
        }
        if(null == value) return "";

        //转为字符串
        DateTimeFormatter fmtDate = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter fmtDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        if( value instanceof String){
            return QueryUtil.toUnderlineStr(field.getName())+"="+"'"+value+"'";
        }else if(value instanceof LocalDate){
            LocalDate date = (LocalDate) value;
            return QueryUtil.toUnderlineStr(field.getName())+"="+"'"+ date.format(fmtDate) +"'";
        }else if(value instanceof LocalDateTime){
            LocalDateTime date = (LocalDateTime) value;
            return QueryUtil.toUnderlineStr(field.getName())+"="+"'"+ date.format(fmtDateTime) +"'";
        }else if(value instanceof Integer){
            return (Integer)value==0?"":QueryUtil.toUnderlineStr(field.getName()) + "=" + String.valueOf(value);
        }else if(value instanceof Long){
            return (long)value==0?"":QueryUtil.toUnderlineStr(field.getName()) + "=" + String.valueOf(value);
        }else if(value instanceof BigDecimal){
            return BigDecimal.ZERO.compareTo((BigDecimal)value)==0?"":QueryUtil.toUnderlineStr(field.getName())+"="+String.valueOf(value);
        }
        return (String) QueryUtil.toUnderlineStr(field.getName())+"="+value;
    }

    /**
     * 取对象属性名并转为字符串（用于sql_insert拼接）
     * @param object 对象
     * @param splitSign 分隔符
     * @return 属性值字符串
     */
    public static String getFieldNameString(Object object,String splitSign){
        StringBuffer s = new StringBuffer();

        List<Field> fields = QueryUtil.getAllFieldList(object.getClass());
        boolean addSplitSign = false;
        for(Field field:fields) {
            //不处理静态变量和常量
            if ( Modifier.isStatic( field.getModifiers() ) || Modifier.isFinal( field.getModifiers() ) ){
                continue;
            }
            String fieldName = QueryUtil.toUnderlineStr( field.getName() );
            if(addSplitSign && QueryUtil.isNotBlankStr(fieldName)) {
                s.append(splitSign).append(" ");
            }else if(QueryUtil.isNotBlankStr(fieldName)){
                addSplitSign = true;
            }
            s.append(fieldName);
        }
        return s.toString();
    }

    /**
     * 取对象属性值并转为字符串（用于sql_insert拼接）
     * @param object 对象
     * @param splitSign 分隔符
     * @return String
     */
    public static String getFieldValueString(Object object,String splitSign){
        StringBuffer s = new StringBuffer();

        List<Field> fields = QueryUtil.getAllFieldList(object.getClass());
        boolean addSplitSign = false;
        for(Field field:fields) {
            //不处理静态变量和常量
            if ( Modifier.isStatic( field.getModifiers() ) || Modifier.isFinal( field.getModifiers() ) ){
                continue;
            }

            Object value = null;
            field.setAccessible(true);//设置可见性为true
            try {
                value = field.get(object);
            }catch (Exception e){
                e.printStackTrace();
            }

            //转为字符串
            String sValue = null;
            DateTimeFormatter fmtDate = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            DateTimeFormatter fmtDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            if(null == value) {
                sValue = "null";
            }else if( value instanceof String){
                sValue = "'"+value+"'";
            }else if(value instanceof LocalDate){
                LocalDate date = (LocalDate) value;
                sValue = "'"+ date.format(fmtDate) +"'";
            }else if(value instanceof LocalDateTime){
                LocalDateTime date = (LocalDateTime) value;
                sValue = "'"+ date.format(fmtDateTime) +"'";
            }else if(value instanceof Integer){
                sValue = String.valueOf(value);
            }else if(value instanceof Long){
                sValue = String.valueOf(value);
            }else if(value instanceof BigDecimal){
                sValue = String.valueOf(value);
            }else {
                sValue = (String) value;
            }

            if(addSplitSign && QueryUtil.isNotBlankStr(sValue.toString())) {
                s.append(" ").append(splitSign);
            }else if(QueryUtil.isNotBlankStr(sValue.toString())){
                addSplitSign = true;
            }
            s.append( sValue );
        }
        return s.toString();
    }

    public static String getSelectPageSql(String sql,int begNum,int pageSize){
        if(begNum >= 0 && pageSize > 0){
            return sql + " LIMIT " + begNum + "," + pageSize;
        }else{
            return sql;
        }
    }

    public static String getSelectCountSql(String sql){
        int selectPos = sql.toUpperCase().indexOf("SELECT "); //第一个select
        int fromPos = sql.toUpperCase().lastIndexOf("FROM "); //最后一个from
        int orderPos = sql.toUpperCase().lastIndexOf("ORDER BY "); //最后一个order by
        int groupPos = sql.toUpperCase().lastIndexOf("GROUP BY "); //最后一个group by

        return "SELECT COUNT(1) FROM " + sql.substring(fromPos+5);
    }

    public static String convertToExpression(String value,String type){
        if("DECIMAL".equals(type) || "INT".equals(type)){
            return value;
        }else if("DATE".equals(type) || "DATETIME".equals(type) ||
                "VARCHAR".equals(type) || "CHAR".equals(type)) {
            return "'"+value+"'";
        }else{
            return "'"+value+"'";
        }
    }
}
