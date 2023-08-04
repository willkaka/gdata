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
     * 获取db的所有数据库名称
     * @param connection 连接
     * @return 所有数据库名称List<String>
     */
    public static List<String> getLibraryNames(Connection connection){
        List<String> dbLibraryList=new ArrayList<String>();
        try {
            DatabaseMetaData dmd = connection.getMetaData();

            if("sqlite".equals(dmd.getDatabaseProductName().toLowerCase())){
                dbLibraryList.add("main");
                return dbLibraryList;
            }

            ResultSet rs = dmd.getCatalogs();
            //List<Map<String,Object>> maplist = getRcdMapFromResultSet(rs);
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

        List<String> tableList = new ArrayList<String>();
        try {
            DatabaseMetaData meta = con.getMetaData();
            ResultSet rs = meta.getTables(catalog, schemaPattern, tableNamePattern, typePattern);
            //List<Map<String,Object>> maplist = getRcdMapFromResultSet(rs);
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
     * 执行sql返回记录集
     * @param connection 数据库连接
     * @param sql 脚本
     * @return 记录集
     */
    public static List<Map<String,Object>> getSqlRecords(Connection connection,String sql){
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
            String sql = SqlUtil.getSelectCountSql("select * from " + table);
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
            String sql = SqlUtil.getSelectPageSql("select * from " + table,begNum,pageSize);
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
        String sqlCount = SqlUtil.getSelectCountSql(sql);
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
            log.error("读取resultset出错！",e);
            throw new DbException("读取resultset出错！");
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
                ResultSetMetaData metaData = set.getMetaData();
                FieldAttr fieldAttr = new FieldAttr();
                for (int fieldNum = 1; fieldNum <= metaData.getColumnCount(); fieldNum++) {
                    if (metaData.getColumnName(fieldNum) != null && !"".equals(metaData.getColumnName(fieldNum))) {
                        String fieldName;
                        if (QueryUtil.isNotBlankStr(metaData.getColumnLabel(fieldNum))) {
                            fieldName = metaData.getColumnLabel(fieldNum);
                        } else {
                            fieldName = metaData.getColumnName(fieldNum);
                        }
                        Object fieldValue = set.getObject(fieldName);
                        switch (fieldName) {
                            case "SCOPE_TABLE": {fieldAttr.setScopeTable(String.valueOf(fieldValue)); break;}
                            case "TABLE_CAT": {fieldAttr.setTableCat(String.valueOf(fieldValue)); break;}
                            case "BUFFER_LENGTH": {fieldAttr.setBufferLength(null==fieldValue?0:(int)fieldValue); break;}
                            case "IS_NULLABLE": {fieldAttr.setIsNullable(String.valueOf(fieldValue)); break;}
                            case "TABLE_NAME": {fieldAttr.setTableName(String.valueOf(fieldValue)); break;}
                            case "COLUMN_DEF": {fieldAttr.setColumnDef(String.valueOf(fieldValue)); break;}
                            case "SCOPE_CATALOG": {fieldAttr.setScopeCatalog(String.valueOf(fieldValue)); break;}
                            case "TABLE_SCHEM": {fieldAttr.setTableSchem(String.valueOf(fieldValue)); break;}
                            case "COLUMN_NAME": {fieldAttr.setColumnName(String.valueOf(fieldValue)); break;}
                            case "NULLABLE": {fieldAttr.setNullable(null==fieldValue?0:(int)fieldValue); break;}
                            case "REMARKS": {fieldAttr.setRemarks(String.valueOf(fieldValue)); break;}
                            case "DECIMAL_DIGITS": {fieldAttr.setDecimalDigits(String.valueOf(fieldValue)); break;}
                            case "NUM_PREC_RADIX": {fieldAttr.setNumPrecRadix(null==fieldValue?0:(int)fieldValue); break;}
                            case "SQL_DATETIME_SUB": {fieldAttr.setSqlDatetimeSub(null==fieldValue?0:(int)fieldValue); break;}
                            case "IS_GENERATEDCOLUMN": {fieldAttr.setIsGeneratedcolumn(String.valueOf(fieldValue)); break;}
                            case "IS_AUTOINCREMENT": {fieldAttr.setIsAutoincrement(String.valueOf(fieldValue)); break;}
                            case "SQL_DATA_TYPE": {fieldAttr.setSqlDataType(null==fieldValue?0:(int)fieldValue); break;}
                            case "CHAR_OCTET_LENGTH": {fieldAttr.setCharOctetLength(null==fieldValue?0:(int)fieldValue); break;}
                            case "ORDINAL_POSITION": {fieldAttr.setOrdinalPosition(null==fieldValue?0:(int)fieldValue); break;}
                            case "SCOPE_SCHEMA": {fieldAttr.setScopeSchema(String.valueOf(fieldValue)); break;}
                            case "SOURCE_DATA_TYPE": {fieldAttr.setSourceDataType(String.valueOf(fieldValue)); break;}
                            case "DATA_TYPE": {fieldAttr.setDataType(String.valueOf(fieldValue)); break;}
                            case "TYPE_NAME": {fieldAttr.setTypeName(String.valueOf(fieldValue)); break;}
                            case "COLUMN_SIZE": {fieldAttr.setColumnSize(null==fieldValue?0:(int)fieldValue); break;}
                        }
                    }
                }
                fieldAttrMap.put(fieldAttr.getColumnName(),fieldAttr);
            }
        }catch (SQLException e){
            log.error("读取resultset出错！",e);
            throw new DbException("读取resultset出错！");
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
            log.error("读取resultset出错！",e);
            throw new DbException("读取resultset出错！");
        }
        return fieldAttrMap;
    }

    private static List<FieldAttr> getFieldAttrFromResultSet(ResultSet set){
        List<FieldAttr> listMap = new ArrayList<>();

        if(set == null) return listMap;

        //取记录
        try {
            while (set.next()) {
                ResultSetMetaData metaData = set.getMetaData();
                FieldAttr fieldAttr = new FieldAttr();
                for (int fieldNum = 1; fieldNum <= metaData.getColumnCount(); fieldNum++) {
                    if (metaData.getColumnName(fieldNum) != null && !"".equals(metaData.getColumnName(fieldNum))) {
                        String fieldName;
                        if (QueryUtil.isNotBlankStr(metaData.getColumnLabel(fieldNum))) {
                            fieldName = metaData.getColumnLabel(fieldNum);
                        } else {
                            fieldName = metaData.getColumnName(fieldNum);
                        }
                        Object fieldValue = set.getObject(fieldName);
                        if(null == fieldValue) continue;
                        switch (fieldName) {
                            case "SCOPE_TABLE": {fieldAttr.setScopeTable(String.valueOf(fieldValue)); break;}
                            case "TABLE_CAT": {fieldAttr.setTableCat(String.valueOf(fieldValue)); break;}
                            case "BUFFER_LENGTH": {fieldAttr.setBufferLength((int)fieldValue); break;}
                            case "IS_NULLABLE": {fieldAttr.setIsNullable(String.valueOf(fieldValue)); break;}
                            case "TABLE_NAME": {fieldAttr.setTableName(String.valueOf(fieldValue)); break;}
                            case "COLUMN_DEF": {fieldAttr.setColumnDef(String.valueOf(fieldValue)); break;}
                            case "SCOPE_CATALOG": {fieldAttr.setScopeCatalog(String.valueOf(fieldValue)); break;}
                            case "TABLE_SCHEM": {fieldAttr.setTableSchem(String.valueOf(fieldValue)); break;}
                            case "COLUMN_NAME": {fieldAttr.setColumnName(String.valueOf(fieldValue)); break;}
                            case "NULLABLE": {fieldAttr.setNullable((int)fieldValue); break;}
                            case "REMARKS": {fieldAttr.setRemarks(String.valueOf(fieldValue)); break;}
                            case "DECIMAL_DIGITS": {fieldAttr.setDecimalDigits(String.valueOf(fieldValue)); break;}
                            case "NUM_PREC_RADIX": {fieldAttr.setNumPrecRadix((int)fieldValue); break;}
                            case "SQL_DATETIME_SUB": {fieldAttr.setSqlDatetimeSub((int)fieldValue); break;}
                            case "IS_GENERATEDCOLUMN": {fieldAttr.setIsGeneratedcolumn(String.valueOf(fieldValue)); break;}
                            case "IS_AUTOINCREMENT": {fieldAttr.setIsAutoincrement(String.valueOf(fieldValue)); break;}
                            case "SQL_DATA_TYPE": {fieldAttr.setSqlDataType((int)fieldValue); break;}
                            case "CHAR_OCTET_LENGTH": {fieldAttr.setCharOctetLength((int)fieldValue); break;}
                            case "ORDINAL_POSITION": {fieldAttr.setOrdinalPosition((int)fieldValue); break;}
                            case "SCOPE_SCHEMA": {fieldAttr.setScopeSchema(String.valueOf(fieldValue)); break;}
                            case "SOURCE_DATA_TYPE": {fieldAttr.setSourceDataType(String.valueOf(fieldValue)); break;}
                            case "DATA_TYPE": {fieldAttr.setDataType(String.valueOf(fieldValue)); break;}
                            case "TYPE_NAME": {fieldAttr.setTypeName(String.valueOf(fieldValue)); break;}
                            case "COLUMN_SIZE": {fieldAttr.setColumnSize((int)fieldValue); break;}
                        }
                    }
                }
                listMap.add(fieldAttr);
            }
        }catch (SQLException e){
            log.error("读取resultset出错！",e);
            throw new DbException("读取resultset出错！");
        }
        return listMap;
    }
}