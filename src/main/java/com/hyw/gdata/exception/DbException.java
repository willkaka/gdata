package com.hyw.gdata.exception;

/**
 * 统一的业务异常处理基类
 * 框架已经做了统一异常处理
 * 所有的业务异常可继承此类通过抛出异常的方式来做业务校验等操作
 */
public class DbException extends DbFastRuntimeException {

    public DbException(String message) { super("9998", message); }

    public DbException(String code, String message) {
        super(code, message);
    }

    public DbException(String message, Exception e) {
        super(message, e);
    }

    public DbException(String code, String message, Exception e) {
        super(code, message, e);
    }

    public static void trueThrow(boolean b, String message, Object... params){
        if(!b) return;
        String msg = message;
        for(Object param:params){
            msg = msg.replaceFirst("\\{}",param.toString());
        }
        throw new DbException(msg);
    }
}
