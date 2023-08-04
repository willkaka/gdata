package com.hyw.gdata;

public class GetDataWrapper<T> {

    /**
     * 数据库表映射实体类
     */
    protected T entity;

    /**
     * 实体类型
     */
    protected Class<T> entityClass;

    public GetDataWrapper() {
        this(null,  null);
    }

    public GetDataWrapper(T entity) {
        this(entity, null);
    }

    public GetDataWrapper(T entity, String... column) {
//        this.sqlSelect = column;
        this.entity = entity;
//        this.initNeed();
    }



    public T getEntity() {
        return entity;
    }


}
