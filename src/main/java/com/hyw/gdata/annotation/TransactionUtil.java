package com.hyw.gdata.annotation;

import com.hyw.gdata.DataService;
import com.hyw.gdata.dto.TransactionalInfo;
import com.hyw.gdata.exception.DbException;
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
        transactionalInfo.setThreadId(Thread.currentThread().getId());
        transactionalInfo.setMethod(method);
        transactionalInfo.setBegTime(begTime);
        transactionalInfo.setThrowableList(throwableList);
        transactionalInfoList.add(transactionalInfo);
        log.info("开启事务");
    }

    // 提交事务
    public static void commit(Method method,LocalDateTime begTime) {
        List<TransactionalInfo> transactionalInfoList = DataService.getTransactionalInfoList();
        for(TransactionalInfo transactionalInfo:transactionalInfoList){
            //同线程号、同方法、同时间
            if(!transactionalInfo.getThreadId().equals(Thread.currentThread().getId())) continue;
            if(!Objects.equals(method,transactionalInfo.getMethod())) continue;
            if(!begTime.equals(transactionalInfo.getBegTime())) continue;

            List<Connection> connectionList = transactionalInfo.getConnectionList();
            for(Connection connection:connectionList){
                try {
                    connection.commit();
                }catch (Exception e){
                    throw new DbException("提交事务失败",e);
                }
            }
            transactionalInfoList.remove(transactionalInfo);
            break;
        }
        log.info("提交事务成功");
    }

    // 回滚事务
    public static void rollback(Method method,LocalDateTime begTime) {
        List<TransactionalInfo> transactionalInfoList = DataService.getTransactionalInfoList();
        for(TransactionalInfo transactionalInfo:transactionalInfoList){
            //同线程号、同方法、同时间
            if(!transactionalInfo.getThreadId().equals(Thread.currentThread().getId())) continue;
            if(!Objects.equals(method,transactionalInfo.getMethod())) continue;
            if(!begTime.equals(transactionalInfo.getBegTime())) continue;

            List<Connection> connectionList = transactionalInfo.getConnectionList();
            for(Connection connection:connectionList){
                try {
                    connection.rollback();
                }catch (Exception e){
                    throw new DbException("回滚事务失败",e);
                }
            }
            transactionalInfoList.remove(transactionalInfo);
            break;
        }
        log.info("已回滚事务...");
    }
}
