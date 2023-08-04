package com.hyw.gdata.utils;

import com.alibaba.fastjson.JSONObject;
import com.hyw.gdata.exception.DbException;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JdbcUtil {

    /**
     * 执行sql返回记录集
     * @param connection 数据库连接
     * @param sql 脚本
     * @return 记录集
     */
    public static List<Map<String,Object>> getSqlRecords(Connection connection, String sql){
        if(connection == null){
            log.error("数据库连接不允许为null！");
            return null;
        }
        if(QueryUtil.isBlankStr(sql)){
            log.error("输入sql语句不允为空！");
            return null;
        }

        List<Map<String,Object>> listMap = new ArrayList<>();
        try {
            Statement statement = connection.createStatement();
            ResultSet set = statement.executeQuery(sql);
            if(set != null) {
                //取记录
                while(set.next()) {
                    ResultSetMetaData metaData = set.getMetaData();
                    Map<String, Object> map = new LinkedHashMap<>();
                    for (int fieldNum = 1; fieldNum <= metaData.getColumnCount(); fieldNum++) {
                        if (metaData.getColumnName(fieldNum) != null && !"".equals(metaData.getColumnName(fieldNum))) {
                            String fieldName;
                            if (QueryUtil.isNotBlankStr(metaData.getColumnLabel(fieldNum))) {
                                fieldName = metaData.getColumnLabel(fieldNum);
                            } else {
                                fieldName = metaData.getColumnName(fieldNum);
                            }
                            Object fieldValue = set.getObject(fieldName);
                            map.put(fieldName, fieldValue);
                        }
                    }
                    listMap.add(map);
                }
            }
            if(set != null) set.close();
            statement.close();
        }catch(Exception e){
            log.error("执行SQL({})出错！",sql,e);
        }finally {
            //DbUtil.closeConnection(connection);
        }
        return listMap;
    }

    /**
     * 执行update语句
     * @param connection 数据库连接
     * @param sql insert/update语句
     * @return boolean成功为真
     */
    public static int updateBySql(Connection connection, String sql){
        int updateCnt=0;
        try {
            Statement statement = connection.createStatement();
            updateCnt = statement.executeUpdate(sql);
            statement.close();
//            connection.commit();
        }catch(Exception e){
            log.error("执行sql语句({})出错！",sql,e);
        }
        return updateCnt;
    }

    /**
     * 执行update语句
     * @param connection 数据库连接
     * @param sqlList insert/update语句
     * @return boolean成功为真
     */
    public static int updateBySql(Connection connection,List<String> sqlList){
        int updateCnt=0;
        //创建Statement
        Statement statement;
        try {
            statement = connection.createStatement();
        }catch(Exception e){
            throw new DbException("数据库连接异常！",e);
        }
        //执行sql
        for (String sql : sqlList) {
            try {
                updateCnt = updateCnt + statement.executeUpdate(sql);
            }catch (Exception e){
                throw new DbException("执行Sql("+sql+")异常！",e);
            }
        }
        //关闭Statement
        try {
            if (statement != null) statement.close();
        }catch (Exception e){
            throw new DbException("关闭Statement异常！",e);
        }
        return updateCnt;
    }


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
     * 生成单笔记录的Insert语句
     * @param objectList 对象
     * @return sql
     */
    public static <T> String getInsertSql(List<T> objectList) {
        if (null == objectList) return null;
        List<Field> fieldList = QueryUtil.getAllFieldList(objectList.get(0).getClass());
        StringBuilder sql = new StringBuilder();
        String tableName = QueryUtil.toUnderlineStr(objectList.get(0).getClass().getSimpleName());

        sql.append("INSERT INTO ").append(tableName).append(" (");
        StringBuilder sqlFields = new StringBuilder();
        for (Field field : fieldList) {
            String fieldName = QueryUtil.toUnderlineStr(field.getName());
//            String fieldType = field.getType().getTypeName();
            //拼接字段名称
            if (QueryUtil.isNotBlankStr(sqlFields.toString())) sqlFields.append(",");
            sqlFields.append(fieldName);
        }

        StringBuilder sqlValues = new StringBuilder();
        for (Object object:objectList){
            JSONObject jsonObject = (JSONObject) JSONObject.toJSON(object);
            if (QueryUtil.isNotBlankStr(sqlValues.toString())) {
                sqlValues.append(",(");
            }else {
                sqlValues.append("(");
            }
            StringBuilder sqlValue = new StringBuilder();
            for (Field field : fieldList) {
                String fieldType = field.getType().getTypeName();
                //拼接字段值
                if (QueryUtil.isNotBlankStr(sqlValue.toString())) sqlValue.append(",");
                sqlValue.append(getFieldValue(fieldType, jsonObject.get(field.getName())));
            }
            sqlValues.append(sqlValue).append(")");
        }

        sql.append(sqlFields).append(") VALUES").append(sqlValues);
//        System.out.println(sql);
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
}
