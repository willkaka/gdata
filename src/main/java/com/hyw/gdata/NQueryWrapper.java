package com.hyw.gdata;

import com.hyw.gdata.utils.QFunction;
import com.hyw.gdata.utils.QueryUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;

@Setter
@Getter
@Accessors(chain = true)
@SuppressWarnings("unchecked")
public class NQueryWrapper<T> {

    private Connection connection;
    private Map<String,Class<T>> classMap = new LinkedHashMap<>();
    private List<String> selectFieldList = new ArrayList<>();
    private List<UpdCondition> conditionList = new ArrayList<>();
    private List<Field> classFieldList = new ArrayList<>();
    private List<GroupInfo> groupInfoList = new ArrayList<>();
    private List<OrderInfo> orderInfoList = new ArrayList<>();
    private String lastString;
    private String sql = null;

    private int totalCnt=0;  //总记录
    private int pageSize=0;  //每页记录数
    private int totalPage=0; //总页数
    private int curRecord=0; //当前记录号
    private int curPage=0;   //当前页数

    public String getSql(){
        if(QueryUtil.isNotBlankStr(this.sql)) return this.sql;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        int condIndex=0;
        if(QueryUtil.isNotEmptyList(selectFieldList)) {
            for (String fieldName : selectFieldList) {
                if (condIndex > 0) sql.append(", ");
                sql.append(fieldName);
                condIndex ++;
            }
        }else {
            sql.append("*");
        }
        sql.append(" FROM ");

        int mapIndex=0;
        // From tables
        for(String tableName:classMap.keySet()){
            if(mapIndex>0) sql.append(",");
            sql.append(QueryUtil.toUnderlineStr(tableName));
            mapIndex++;
        }

        // Where conditions
        if(QueryUtil.isNotEmptyList(conditionList)) sql.append(" WHERE ");
        condIndex=0;
        for(UpdCondition condition:conditionList){
            if(condIndex>0) sql.append(" AND ");
            sql.append(QueryUtil.toUnderlineStr(condition.getColumn().toString()))
                    .append(" ").append(condition.getSymbol())
                    .append(" ").append(condition.getValue());
            condIndex++;
        }

        // Group By
        condIndex=0;
        if(QueryUtil.isNotEmptyList(groupInfoList)) sql.append(" GROUP BY ");
        for(GroupInfo orderInfo:groupInfoList){
            if(condIndex>0) sql.append(", ");
            sql.append(orderInfo.getColumn().toString());
            condIndex++;
        }

        // Order By
        condIndex=0;
        if(QueryUtil.isNotEmptyList(orderInfoList)) sql.append(" ORDER BY ");
        for(OrderInfo orderInfo:orderInfoList){
            if(condIndex>0) sql.append(", ");
            sql.append(orderInfo.getColumn().toString())
                    .append(" ").append(orderInfo.getOrderKey());
            condIndex++;
        }

        //分页
        if(pageSize>0){
            sql.append(" LIMIT ").append(curPage*pageSize).append(",").append(pageSize);
        }

        //加在sql后方字符 eg. limit x
        if(QueryUtil.isNotBlankStr(lastString)){
            sql.append(" ").append(lastString);
        }
//        System.out.println(sql);
        return sql.toString();
    }

    public String getCountSql(){
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append("COUNT(1)");
        sql.append(" FROM ");

        int mapIndex=0;
        // From tables
        for(String tableName:classMap.keySet()){
            if(mapIndex>0) sql.append(",");
            sql.append(QueryUtil.toUnderlineStr(tableName));
            mapIndex++;
        }

        // Where conditions
        if(QueryUtil.isNotEmptyList(conditionList)) sql.append(" WHERE ");
        int condIndex=0;
        for(UpdCondition condition:conditionList){
            if(condIndex>0) sql.append(" AND ");
            sql.append(QueryUtil.toUnderlineStr(condition.getColumn().toString()))
                    .append(" ").append(condition.getSymbol())
                    .append(" ").append(condition.getValue());
            condIndex++;
        }

        // Group By
        condIndex=0;
        if(QueryUtil.isNotEmptyList(groupInfoList)) sql.append(" GROUP BY ");
        for(GroupInfo orderInfo:groupInfoList){
            if(condIndex>0) sql.append(", ");
            sql.append(orderInfo.getColumn().toString());
            condIndex++;
        }
//        System.out.println(sql);
        return sql.toString();
    }

    @SafeVarargs
    public final <A,B> NQueryWrapper<T> selectFields(QFunction<A, B>... functions) {
        for (QFunction<A, B> function : functions) {
            selectFieldList.add(QueryUtil.toUnderlineStr(QueryUtil.getImplMethodName(function).replace("get", "")));
        }
        return this;
    }

    public final <A,B> NQueryWrapper<T> selectFields(String userDefinedField) {
        selectFieldList.add(userDefinedField);
        return this;
    }

    public <A,B> NQueryWrapper<T> eq(Function<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"=",getValue(value));
        return this;
    }

    public <A,B> NQueryWrapper<T> eq(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"=",getValue(value));
        return this;
    }

    public <A,B> NQueryWrapper<T> eq(boolean isTrue, QFunction<A, B> function, Object value) {
        if(isTrue) {
            putTable(function);
            putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"=",getValue(value));
        }
        return this;
    }

    public <A,B> NQueryWrapper<T> eq(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toUnderlineStr(QueryUtil.getImplMethodName(function1).replace("get","")),"=",
                QueryUtil.toUnderlineStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NQueryWrapper<T> ne(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"<>",getValue(value));
        return this;
    }

    public <A,B> NQueryWrapper<T> ne(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),"<>",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NQueryWrapper<T> gt(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),">",getValue(value));
        return this;
    }

    public <A,B> NQueryWrapper<T> gt(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),">",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NQueryWrapper<T> ge(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),">=",getValue(value));
        return this;
    }

    public <A,B> NQueryWrapper<T> ge(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),">=",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NQueryWrapper<T> lt(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"<",value);
        return this;
    }

    public <A,B> NQueryWrapper<T> lt(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),"<",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NQueryWrapper<T> le(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"<=",getValue(value));
        return this;
    }

    public <A,B> NQueryWrapper<T> le(QFunction<A, B> function1, QFunction<A, B> function2) {
        putTable(function1);
        putTable(function2);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function1).replace("get","")),"<=",
                QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function2).replace("get","")));
        return this;
    }

    public <A,B> NQueryWrapper<T> in(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"in",getValue(value));
        return this;
    }

    public <A,B> NQueryWrapper<T> notIn(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"not in",getValue(value));
        return this;
    }

    public <A,B> NQueryWrapper<T> like(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"like",
                getValue("%"+String.valueOf(value)+"%"));
        return this;
    }

    public <A,B> NQueryWrapper<T> likeRight(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"like",
                getValue(String.valueOf(value)+"%"));
        return this;
    }

    public <A,B> NQueryWrapper<T> likeLeft(QFunction<A, B> function, Object value) {
        putTable(function);
        putCondition(QueryUtil.toCamelCaseStr(QueryUtil.getImplMethodName(function).replace("get","")),"like",
                getValue("%"+String.valueOf(value)));
        return this;
    }

    @SafeVarargs
    public final <A,B> NQueryWrapper<T> groupBy(QFunction<A, B>... functions) {
        for (QFunction<A, B> function : functions) {
            putTable(function);
            GroupInfo groupInfo = new GroupInfo();
            groupInfo.setSeq(groupInfoList.size() + 1);
            groupInfo.setColumn(QueryUtil.toUnderlineStr(QueryUtil.getImplMethodName(function).replace("get", "")));
            groupInfoList.add(groupInfo);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public <A,B> NQueryWrapper<T> orderByAsc(QFunction<A, B>... functions) {
        for (QFunction<A, B> function : functions) {
            putTable(function);
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setSeq(orderInfoList.size() + 1);
            orderInfo.setOrderKey("Asc");
            orderInfo.setColumn(QueryUtil.toUnderlineStr(QueryUtil.getImplMethodName(function).replace("get", "")));
            orderInfoList.add(orderInfo);
        }
        return this;
    }

    public <A,B> NQueryWrapper<T> orderByDesc(QFunction<A, B>... functions) {
        for (QFunction<A, B> function : functions) {
            putTable(function);
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setSeq(orderInfoList.size() + 1);
            orderInfo.setOrderKey("Desc");
            orderInfo.setColumn(QueryUtil.toUnderlineStr(QueryUtil.getImplMethodName(function).replace("get", "")));
            orderInfoList.add(orderInfo);
        }
        return this;
    }

    public NQueryWrapper<T> last(String lastSql) {
        this.lastString = lastSql;
        return this;
    }

    public NQueryWrapper<T> setTable(Class<T> clazz){
        classMap.put(clazz.getSimpleName(),clazz);
        return this;
    }

    private void putCondition(String fieldName,String symbol,Object value){
        UpdCondition condition = new UpdCondition();
        condition.setColumn(fieldName).setSymbol(getSymbol(symbol,value)).setValue(value);
        conditionList.add(condition);
    }

    private String getSymbol(String curSymbol,Object value){
        if("=".equals(curSymbol) && (value == null ||"null".equalsIgnoreCase(value.toString()))){
            return "is";
        }else if("<>".equals(curSymbol) && (value == null ||"null".equalsIgnoreCase(value.toString()))){
            return "not is";
        }else{
            return curSymbol;
        }
    }

    private <A,B> void putTable(Function<A, B> function){
        Class clazz = QueryUtil.getClassWithFunc(function);
        String clazzName = clazz.getSimpleName();
        if(!classMap.containsKey(clazzName)){
            classMap.put(clazzName,clazz);
        }
    }

    private <A,B> void putTable(QFunction<A, B> function){
        Class clazz = QueryUtil.getClassWithFunc(function);
        String clazzName = clazz.getSimpleName();
        if(!classMap.containsKey(clazzName)){
            classMap.put(clazzName,clazz);
        }
    }

//    private <A,B> SerializedLambda putTable(QFunction<A, B> function){
////        SerializedLambda SerializedUtil = QueryUtil.resolve(function);
////        Class clazz = SerializedUtil.getImplClass();
//        Class clazz = QueryUtil.getClassWithFunc(function);
////        Class clazz = com.baomidou.mybatisplus.core.toolkit.QueryUtil.toClassConfident(SerializedUtil.getImplClass().replace('/', '.'));
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
        if(value == null){
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
class Condition{
    private Object column;
    private String symbol;
    private Object value;
}

@Setter
@Getter
@Accessors(chain = true)
class GroupInfo{
    private int seq;
    private Object column;
}

@Setter
@Getter
@Accessors(chain = true)
class OrderInfo{
    private int seq;
    private Object column;
    private String orderKey; //Asc/Desc
}