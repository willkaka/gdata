package com.hyw.gdata.dto;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors( chain = true )
public class FieldAttr implements Cloneable{
    private String scopeTable;
    private String tableCat;
    private Integer bufferLength;
    private String isNullable;
    private String tableName;
    private String columnDef;
    private String scopeCatalog;
    private String tableSchem;
    private String columnName;
    private Integer nullable;
    private String remarks;
    private String decimalDigits;
    private Integer numPrecRadix;
    private Integer sqlDatetimeSub;
    private String isGeneratedcolumn;
    private String isAutoincrement;
    private Integer sqlDataType;
    private Integer charOctetLength;
    private Integer ordinalPosition;
    private String scopeSchema;
    private String sourceDataType;
    private String dataType;
    private String typeName;
    private Integer columnSize;

    private String typeClass; //字段类型类
    private Object value;     //初始值
    private Object curValue;  //当前值
    private boolean isKeyField;//是否为主键

    @Override
    public FieldAttr clone() {
        FieldAttr fieldAttr = null;
        try{
            fieldAttr = (FieldAttr)super.clone();
        }catch(CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return fieldAttr;
    }
}
