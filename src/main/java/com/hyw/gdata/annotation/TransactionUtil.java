package com.hyw.gdata.annotation;

import com.hyw.gdata.DataService;
import com.hyw.dataservice.dto.TransactionalInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Component
@Scope("prototype")
@Slf4j
public class TransactionUtil {

    // 开启事务
    public static void begin(Method method, LocalDateTime begTime,Class<? extends Throwable>[] throwableList) {
        List<TransactionalInfo> transactionalInfoList = DataService.getTransactionalInfoList();
        TransactionalInfo transactionalInfo = new TransactionalInfo();
        transactionalInfo.setTransactionId(UUID.randomUUID().toString());
        transactionalInfo.setMethod(method);
        transactionalInfo.setBegTime(begTime);
        transactionalInfo.setThrowableList(throwableList);
        transactionalInfoList.add(transactionalInfo);
        System.out.println("开启事务");
    }

    // 提交事务
    public static void commit(Method method,LocalDateTime begTime) {
        List<TransactionalInfo> transactionalInfoList = DataService.getTransactionalInfoList();
        for(TransactionalInfo transactionalInfo:transactionalInfoList){
            if(Objects.equals(method,transactionalInfo.getMethod()) && begTime.equals(transactionalInfo.getBegTime())){
                List<Connection> connectionList = transactionalInfo.getConnectionList();
                for(Connection connection:connectionList){
                    try {
                        connection.commit();
                    }catch (Exception e){
                        log.error("提交事务失败",e);
                    }
                }
            }
        }
        System.out.println("提交事务成功");
    }

    // 回滚事务
    public static void rollback(Method method,LocalDateTime begTime) {
        List<TransactionalInfo> transactionalInfoList = DataService.getTransactionalInfoList();
        for(TransactionalInfo transactionalInfo:transactionalInfoList){
            if(Objects.equals(method,transactionalInfo.getMethod()) && begTime.equals(transactionalInfo.getBegTime())){
                List<Connection> connectionList = transactionalInfo.getConnectionList();
                for(Connection connection:connectionList){
                    try {
                        connection.rollback();
                    }catch (Exception e){
                        log.error("提交事务失败",e);
                    }
                }
            }
        }
        System.out.println("回滚事务...");
    }
}
