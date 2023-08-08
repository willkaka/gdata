package com.hyw.gdata.utils;

import com.hyw.gdata.exception.DbException;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SqlExecUtil {

    /**
     * 执行sql返回记录集
     * @param connection 数据库连接
     * @param sql 脚本
     * @return 记录集
     */
    public static List<Map<String,Object>> queryListMapBySql(Connection connection, String sql){
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
    public static int updateBySqlList(Connection connection, List<String> sqlList){
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
}
