package com.hyw.gdata.annotation;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionStatus;

import java.lang.reflect.Method;
import java.time.LocalDateTime;

/**
 * 切面类封装事务逻辑
 */
@Aspect// 申明为切面
@Component
@Slf4j
public class AopTransaction {

    private TransactionStatus transactionStatus;
    /**
     * 环绕通知 在方法之前和之后处理事情
     * @param pjp 切入点
     */
    @Around("@annotation(NTransactional)")
    public Object around(ProceedingJoinPoint pjp) throws Throwable {
        log.info("进入AopTransaction.around()");
        // 1.获取方法的注解
        //1. 获取代理对对象的方法
        String methodName = pjp.getSignature().getName();
        //2. 获取目标对象
        Class<?> classTarget = pjp.getTarget().getClass();
        //3. 获取目标对象类型
        Class<?>[] par = ((MethodSignature) pjp.getSignature()).getParameterTypes();
        //4. 获取目标对象方法
        Method objMethod = classTarget.getMethod(methodName, par);
        //5. 获取该方法上的事务注解
        NTransactional annotation = objMethod.getDeclaredAnnotation(NTransactional.class);

        // 2.判断是否需要开启事务
        LocalDateTime begTime = LocalDateTime.now();
        Class<? extends Throwable>[] throwableList = new Class[0];
        if(annotation!=null) throwableList = annotation.rollbackWhen();
        begin(annotation, objMethod, begTime, throwableList);

        // 3.调用目标代理对象方法
        Object rtnObject = null;
        try {
            rtnObject = pjp.proceed(pjp.getArgs());
        }catch (Exception e){
            //发生异常，回滚事务
            if(throwableList==null || throwableList.length <= 0){
                TransactionUtil.rollback(objMethod,begTime);
                return null;
            }
            for (Class<? extends Throwable> eClass : throwableList) {
                if(eClass.equals(e.getClass())) {
//                if (eClass.isAssignableFrom(e.getClass())) {
                    // rollback
                    TransactionUtil.rollback(objMethod,begTime);
                    return null;
                }
            }
        }

        // 4.判断关闭事务
        if(annotation!=null) TransactionUtil.commit(objMethod,begTime);
        return rtnObject;
    }

    /** 开启事务  */
    private void begin(NTransactional annotation, Method method, LocalDateTime begTime, Class<? extends Throwable>[] throwableList) {
        if(annotation == null) return;
        TransactionUtil.begin(method,begTime,throwableList);
    }

    /**
     * 异常通知进行 回滚事务
     */
//    @AfterThrowing("@annotation(com.hyw.webSite.dbservice.annotation.NTransactional)")
//    @AfterThrowing("execution(* com.hyw.webSite.*.*(..))")
//    public void afterThrowing(Throwable a) {
//        // 获取当前事务 直接回滚
//        //TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
//        //if(transactionStatus != null) TransactionUtil.rollback();
//        String xx = "xx";
//
//
//    }
}
