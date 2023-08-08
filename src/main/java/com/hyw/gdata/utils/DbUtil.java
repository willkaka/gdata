package com.hyw.gdata.utils;

import com.hyw.gdata.constant.DbConstant;
import com.hyw.gdata.dto.FieldAttr;
import com.hyw.gdata.exception.DbException;
import com.hyw.gdata.exception.DbThrow;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DbUtil {


    /**
     * 取DB数据库连接
     * @param driver 驱动
     * @param url 地址
     * @param user 用户名
     * @param password 密码
     * @return Connection
     */
    public static Connection getConnection(String dbType,String driver,String url,String attr,String lib,String user,String password) {
        String dbUrl ="";
        if(dbType.equalsIgnoreCase(DbConstant.DB_TYPE_MYSQL)) {
            dbUrl = url
                    + (QueryUtil.isNotBlankStr(lib) ? "/" + lib : "")
                    + (QueryUtil.isNotBlankStr(attr) ? "?" + attr : "");
        }else if(dbType.equalsIgnoreCase(DbConstant.DB_TYPE_ORACLE)) {
            dbUrl = url + (QueryUtil.isNotBlankStr(lib) ? ":" + lib : "");
//        }else if(dbType.equalsIgnoreCase(Constant.DB_TYPE_EXCEL)){
//            dbUrl = url + (QueryUtil.isNotBlankStr(lib) ? "DBQ="+ lib : "");
        }if(dbType.equalsIgnoreCase(DbConstant.DB_TYPE_SQLITE)){
            if("main".equals(lib)) {
                dbUrl = url;
            }else{
                dbUrl = url
                        + (QueryUtil.isNotBlankStr(lib) ? ":" + lib : "");
            }
        }

        try {
            Class.forName(driver);
            return DriverManager.getConnection(dbUrl, user, password);
        } catch (Exception e) {
            log.error("数据库连接出错！driver({}),url({}),user({}),password({})",driver,dbUrl,user,password,e);
            throw new DbException("数据库连接出错！");
        }
    }

    /**
     * 关闭数据库连接
     * @param connection 数据库连接
     */
    public static void closeConnection(Connection connection){
        try{
            if(connection != null) connection.close();
        }catch (Exception e){
            log.error("关闭数据库连接出错！",e);
        }
    }

    /**
     * 获取db的所有数据库名称
     * @param connection 连接
     * @return 所有数据库名称List<String>
     */
    public static List<String> getLibraryNames(Connection connection){
        List<String> dbLibraryList=new ArrayList<>();
        try {
            DatabaseMetaData dmd = connection.getMetaData();

            if("sqlite".equalsIgnoreCase(dmd.getDatabaseProductName())){
                dbLibraryList.add("main");
                return dbLibraryList;
            }

            ResultSet rs = dmd.getCatalogs();
            while (rs.next()) {
                dbLibraryList.add(rs.getString(1));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return dbLibraryList;
    }

    public static List<String> getTableNames(Connection con,String libName) {
        String catalog           = libName;  //表所在的编目
        String schemaPattern     = null;  //表所在的模式
        String tableNamePattern  = null; //匹配表名
        //指出返回何种表的数组("TABLE"、"VIEW"、"SYSTEM TABLE"， "GLOBAL TEMPORARY"，"LOCAL  TEMPORARY"，"ALIAS"，"SYSNONYM")
        String[] typePattern = new String[] { "TABLE" };

        List<String> tableList = new ArrayList<>();
        try {
            DatabaseMetaData meta = con.getMetaData();
            ResultSet rs = meta.getTables(catalog, schemaPattern, tableNamePattern, typePattern);
            while (rs.next()) {
                tableList.add(rs.getString(3)); //rs.getString(3)表名，rs.getString(2)表所属用户名
            }
        } catch (Exception e) {
            throw new DbException("取数据库的所有表格名称出错！");
        }
        return tableList;
    }

    public static List<FieldAttr> getFieldAttr(Connection connection, String dbName, String libName, String tableName) {
        String   catalog           = libName;  //表所在的编目
        String   schemaPattern     = dbName;  //表所在的模式
        String   tableNamePattern  = tableName; //匹配表名
        String   columnNamePattern = null; //

        List<FieldAttr> fieldsMap = new ArrayList<>();
        try{
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet result = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            fieldsMap = getFieldAttrFromResultSet(result);
        }catch(SQLException e){
            log.error("取数据表结构失败！",e);
            throw new DbException("取数据表结构失败！");
        }
        return fieldsMap;
    }

    public static Map<String,FieldAttr> getFieldAttrMap(Connection connection,String dbName, String libName,String tableName) {
        String   catalog           = libName;  //表所在的编目
        String   schemaPattern     = dbName;  //表所在的模式
        String   tableNamePattern  = tableName; //匹配表名
        String   columnNamePattern = null; //

        Map<String,FieldAttr> fieldsMap;
        try{
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet result = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            fieldsMap = getFieldAttrMapFromResultSet(result);

            List<String> keyList = DbUtil.getTablePrimaryKeys(connection,libName,tableName);
            for(String keyFieldName:keyList){
                fieldsMap.get(keyFieldName).setKeyField(true);
            }
        }catch(SQLException e){
            log.error("取数据表结构失败！",e);
            throw new DbException("取数据表结构失败！");
        }
        return fieldsMap;
    }

    public static List<Map<String,Object>> getFieldInfo(Connection connection,String dbName, String libName,String tableName) {
        String   catalog           = libName;  //表所在的编目
        String   schemaPattern     = dbName;  //表所在的模式
        String   tableNamePattern  = tableName; //匹配表名
        String   columnNamePattern = null; //

        List<Map<String,Object>> fieldsMap = new ArrayList<>();
        try{
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet result = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            fieldsMap = getRcdMapFromResultSet(result);
        }catch(SQLException e){
            log.error("取数据表结构失败！",e);
            throw new DbException("取数据表结构失败！");
        }
        return fieldsMap;
    }

    /**
     * 返回表createTableDDL
     * @param connection
     * @param tableName
     * @return
     */
    public static String getTableCreatedDdl(Connection connection,String tableName) {
        String createDDL = null;
        try{
            Statement stmt = (Statement) connection.createStatement();
            String sql = null;
            if(connection.getMetaData().getDriverName().toLowerCase().contains("sqlite")){
                sql = "SELECT '1' temp1,sql FROM sqlite_master WHERE type='table' AND name ='" + tableName +"'";
            }else if(connection.getMetaData().getDriverName().toLowerCase().contains("mysql")){
                sql = "SHOW CREATE TABLE " + tableName;
            }else{
                throw new DbException("暂不支持该数据库("+connection.getMetaData().getDriverName()+")");
            }
            ResultSet rs = stmt.executeQuery(sql);
            if (rs != null && rs.next()) {
                createDDL = rs.getString(2);
            }
            assert rs != null;
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return createDDL;
    }

    /**
     * 返回表注释
     * @param connection
     * @param tableName
     * @return
     */
    public static String getTableComment(Connection connection,String tableName) {
        String tableComment = null;
        try{
            Statement  stmt = (Statement) connection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName);
            if (rs != null && rs.next()) {
                String createDDL = rs.getString(2);
                int index = createDDL.indexOf("COMMENT='");
                if (index < 0) {
                    return "";
                }
                tableComment = createDDL.substring(index + 9);
                tableComment = tableComment.substring(0, tableComment.length() - 1);

            }
            assert rs != null;
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return tableComment;
    }

    /**
     * 返回表结构显示的内容
     * @param connection
     * @param libName
     * @param tableName
     * @return
     */
    public static List<String> getTablePrimaryKeys(Connection connection,String libName, String tableName) {
        String   catalog           = libName;  //表所在的编目
        String   schemaPattern     = null;  //表所在的模式
        String   tableNamePattern  = tableName; //匹配表名

        List<String> primaryKeys = new ArrayList<>();
        try{
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet set = databaseMetaData.getPrimaryKeys(catalog, schemaPattern, tableNamePattern);
            if(set == null) return primaryKeys;
            while(set.next()) {
                primaryKeys.add(set.getString(4));
            }
        }catch(SQLException e){
            log.error("取数据表结构失败！",e);
            throw new DbException("取数据表结构失败！");
        }
        return primaryKeys;
    }


    /**
     * 执行sql返回记录集
     * @param connection 数据库连接
     * @param sql 脚本
     * @return 记录集
     */
    public static List<Map<String,Object>> queryListMapBySql(Connection connection,String sql){
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
            throw new DbException("执行SQL("+sql+")出错！");
        }finally {
            //DbUtil.closeConnection(connection);
        }
        return listMap;
    }

    /**
     * 查询数据表返回记录集
     * @param connection 数据库连接
     * @param table 查询数据表
     * @return 记录集
     */
    public static int getTableRecordCount(Connection connection,String db,String lib,String table){
        DbThrow.isNull(connection,"数据库连接不允许为null！");
        DbThrow.isBlank(table,"查询数据表不允为空！");

        int count = 0;
        try {
            Statement statement = connection.createStatement();
            String sql = SqlGenUtil.getSelectCountSql("select * from " + table);
            ResultSet set = statement.executeQuery(sql);
            if(set != null) {
                //取记录
                if(set.next()) {
                    count = set.getInt(1);
                }
            }
            if(set != null) set.close();
            statement.close();
        }catch(Exception e){
            DbUtil.closeConnection(connection);
            log.error("查询数据表({})出错！",table,e);
            throw new DbException("查询数据表("+table+")出错！");
        }
        return count;
    }

    /**
     * 查询数据表返回记录集
     * @param connection 数据库连接
     * @param table 查询数据表
     * @return 记录集
     */
    public static List<Map<String,FieldAttr>> getTableRecords(Connection connection,String db,String lib,String table,int begNum,int pageSize){
        DbThrow.isNull(connection,"数据库连接不允许为null！");
        DbThrow.isBlank(table,"查询数据表不允为空！");

        List<String> keyList = DbUtil.getTablePrimaryKeys(connection,db,table);
        Map<String,FieldAttr> fieldsMap = DbUtil.getFieldAttrMap(connection,db,lib,table);
        for(String keyFieldName:keyList){
            fieldsMap.get(keyFieldName).setKeyField(true);
        }

        List<Map<String,FieldAttr>> listMap = new ArrayList<>();
        try {
            Statement statement = connection.createStatement();
            String sql = SqlGenUtil.getSelectPageSql("select * from " + table,begNum,pageSize);
            ResultSet set = statement.executeQuery(sql);
            if(set != null) {
                //取记录
                while(set.next()) {
                    ResultSetMetaData metaData = set.getMetaData();
                    Map<String, FieldAttr> map = getFieldAttrMapClone(fieldsMap);
                    //fieldsMap.forEach((key,obj) -> map.put(key,fieldsMap.get(key).clone()));
                    for (int fieldNum = 1; fieldNum <= metaData.getColumnCount(); fieldNum++) {
                        if (metaData.getColumnName(fieldNum) != null && !"".equals(metaData.getColumnName(fieldNum))) {
                            String fieldName;
                            if (QueryUtil.isNotBlankStr(metaData.getColumnLabel(fieldNum))) {
                                fieldName = metaData.getColumnLabel(fieldNum);
                            } else {
                                fieldName = metaData.getColumnName(fieldNum);
                            }
                            Object fieldValue = set.getObject(fieldName);
                            map.get(fieldName).setValue(fieldValue);
                        }
                    }
                    listMap.add(map);
                }
            }
            if(set != null) set.close();
            statement.close();
        }catch(Exception e){
            DbUtil.closeConnection(connection);
            log.error("查询数据表({})出错！",table,e);
        }
        return listMap;
    }

    /**
     * 取sql查询语句执行后记录数
     * @param connection 数据库连接
     * @param sql sql
     * @return 记录数
     */
    public static int getSqlRecordCount(Connection connection,String sql){
        DbThrow.isNull(connection,"数据库连接不允许为null！");
        DbThrow.isBlank(sql,"sql不允为空！");

        int count = 0;
        String sqlCount = SqlGenUtil.getSelectCountSql(sql);
        try {
            Statement statement = connection.createStatement();
            ResultSet set = statement.executeQuery(sqlCount);
            if(set != null) {
                //取记录
                if(set.next()) {
                    count = set.getInt(1);
                }
            }
            if(set != null) set.close();
            statement.close();
        }catch(Exception e){
            DbUtil.closeConnection(connection);
            log.error("执行sql({})出错！",sqlCount,e);
        }
        return count;
    }

    /**
     * 查询数据表返回记录集
     * @param connection 数据库连接
     * @param sql sql
     * @return 记录集
     */
    public static List<Map<String,FieldAttr>> getSqlRecordsWithFieldAttr(Connection connection,String sql){
        DbThrow.isNull(connection,"数据库连接不允许为null！");

        List<Map<String,FieldAttr>> listMap = new ArrayList<>();
        try {
            Statement statement = connection.createStatement();
            ResultSet set = statement.executeQuery(sql);
            Map<String,FieldAttr> fieldsMap = getSqlFieldAttrMapFromResultSet(connection,set);
            //取记录
            while(set != null && set.next()) {
                ResultSetMetaData metaData = set.getMetaData();
                Map<String, FieldAttr> map = getFieldAttrMapClone(fieldsMap);
                //fieldsMap.forEach((key,obj) -> map.put(key,obj.clone()));
                for (int fieldNum = 1; fieldNum <= metaData.getColumnCount(); fieldNum++) {
                    if (metaData.getColumnName(fieldNum) != null && !"".equals(metaData.getColumnName(fieldNum))) {
                        Object fieldValue = set.getObject(metaData.getColumnLabel(fieldNum));
                        if(null != map.get(metaData.getColumnName(fieldNum))){
                            map.get(metaData.getColumnName(fieldNum)).setValue(fieldValue);
                        }
                    }
                }
                listMap.add(map);
            }
            if(set != null) set.close();
            statement.close();
        }catch(Exception e){
            log.error("查询sql({})出错！",sql,e);
        }finally {
            //DbUtil.closeConnection(connection);
        }
        return listMap;
    }

    private static Map<String,FieldAttr> getFieldAttrMapClone(Map<String,FieldAttr> mapA){
        Map<String,FieldAttr> mapB = new LinkedHashMap<>();
        for(String fieldName:mapA.keySet()){
            mapB.put(fieldName,null==mapA.get(fieldName)?null:mapA.get(fieldName).clone());
        }
        return mapB;
    }

    /**
     * 执行insert/update语句
     * @param connection 数据库连接
     * @param sql insert/update语句
     * @return boolean成功为真
     */
    public static boolean executeSql(Connection connection,String sql){
        DbThrow.isNull(connection,"数据库连接不允许为null！");
        DbThrow.isBlank(sql,"sql不允为空！");

        boolean rtnFlag = false;
        try {
            Statement statement = connection.createStatement();
            rtnFlag = statement.execute(sql);
            statement.close();
        }catch(Exception e){
            log.error("执行sql语句({})出错！",sql,e);
        }finally {
            DbUtil.closeConnection(connection);
        }
        return rtnFlag;
    }

    public static List<Map<String,Object>> getRcdMapFromResultSet(ResultSet set){
        List<Map<String,Object>> listMap = new ArrayList<>();

        if(set == null) return listMap;

        //取记录
        try {
            while (set.next()) {
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
        }catch (SQLException e){
            log.error("读取resultSet出错！",e);
            throw new DbException("读取resultSet出错！");
        }
        return listMap;
    }


    /**
     * 取字段信息
     * @param set ResultSet
     * @return Map<String-字段名,FieldAttr-字段属性>
     */
    private static Map<String,FieldAttr> getFieldAttrMapFromResultSet(ResultSet set){
        Map<String,FieldAttr> fieldAttrMap = new LinkedHashMap<>();
        if(set == null) return fieldAttrMap;
        //取记录
        try {
            while (set.next()) {
                FieldAttr fieldAttr = new FieldAttr(set);
                fieldAttrMap.put(fieldAttr.getColumnName(),fieldAttr);
            }
        }catch (SQLException e){
            log.error("读取resultSet出错！",e);
            throw new DbException("读取resultSet出错！");
        }
        //排序

        return fieldAttrMap;
    }


    /**
     * 取sql字段信息
     * @param set ResultSet
     * @return Map<String-字段名,FieldAttr-字段属性>
     */
    private static Map<String,FieldAttr> getSqlFieldAttrMapFromResultSet(Connection connection,ResultSet set){
        Map<String,FieldAttr> fieldAttrMap = new LinkedHashMap<>();

        Map<String,Map<String,FieldAttr>> tableFieldsMap = new LinkedHashMap<>();
        if(set == null) return fieldAttrMap;
        //取记录
        try {
            ResultSetMetaData metaData = set.getMetaData();
            for (int fieldNum = 1; fieldNum <= metaData.getColumnCount(); fieldNum++) {
                Map<String,FieldAttr> fieldAttrs;
                if(tableFieldsMap.containsKey(metaData.getTableName(fieldNum))){
                    fieldAttrs = tableFieldsMap.get(metaData.getTableName(fieldNum));
                }else{
                    fieldAttrs = getFieldAttrMap(connection,null,metaData.getCatalogName(fieldNum),metaData.getTableName(fieldNum));
                    tableFieldsMap.put(metaData.getTableName(fieldNum),fieldAttrs);
                }
                if (metaData.getColumnName(fieldNum) != null && !"".equals(metaData.getColumnName(fieldNum))) {
                    String fieldName;
//                    if (StringUtils.isNotBlank(metaData.getColumnLabel(fieldNum))) {
//                        fieldName = metaData.getColumnLabel(fieldNum);
//                    } else {
                        fieldName = metaData.getColumnName(fieldNum);
//                    }
                    FieldAttr fieldAttr = fieldAttrs.get(fieldName);
                    fieldAttrMap.put(fieldName,fieldAttr);
                }
            }
        }catch (SQLException e){
            log.error("读取resultSet出错！",e);
            throw new DbException("读取resultSet出错！");
        }
        return fieldAttrMap;
    }

    private static List<FieldAttr> getFieldAttrFromResultSet(ResultSet set){
        List<FieldAttr> listMap = new ArrayList<>();

        if(set == null) return listMap;

        //取记录
        try {
            while (set.next()) {
                FieldAttr fieldAttr = new FieldAttr(set);
                listMap.add(fieldAttr);
            }
        }catch (SQLException e){
            log.error("读取resultSet出错！",e);
            throw new DbException("读取resultSet出错！");
        }
        return listMap;
    }
}