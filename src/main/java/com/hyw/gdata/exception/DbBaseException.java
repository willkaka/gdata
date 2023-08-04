package com.hyw.gdata.exception;


import java.util.Date;

public interface DbBaseException {
    String getCode();

    String[] getArgs();

    void setTime(Date paramDate);

    Date getTime();

    void setClassName(String paramString);

    String getClassName();

    void setMethodName(String paramString);

    String getMethodName();

    void setParameters(String[] paramArrayOfString);

    String[] getParameters();

    void setHandled(boolean paramBoolean);

    boolean isHandled();

    String getMessage();

    void setI18nMessage(String paramString);

    String getI18nMessage();
}
