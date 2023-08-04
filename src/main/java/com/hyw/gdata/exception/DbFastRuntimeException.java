package com.hyw.gdata.exception;

/**
 * 一般用于业务异常集成，统一异常处理拦截此异常。
 * @author chengjie
 */
public class DbFastRuntimeException extends DbBaseRuntimeException {
    private static final long serialVersionUID = -4954118251735823026L;

    public DbFastRuntimeException(String msg) {
        super(msg);
    }

    public DbFastRuntimeException(String code , Object[] args){
        super(code, "",args);
    }

    public DbFastRuntimeException(String code , String defaultMsg , Object[] args){
        super(code,defaultMsg,args);
    }

    public DbFastRuntimeException(String code, String msg) {
        super(code, msg, new Object[0]);
    }

    public DbFastRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public DbFastRuntimeException(String code, String msg, Throwable cause) {
        super(code, msg, cause, new Object[0]);
    }
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
