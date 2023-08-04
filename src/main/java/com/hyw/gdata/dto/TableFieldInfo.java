package com.hyw.gdata.dto;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors( chain = true )
public class TableFieldInfo implements Cloneable{
    private String tableName;
    private String fieldName;
    private Integer fieldSeq;

    //type
    private String fieldType;
    private Integer fieldLength;
    private Integer decimalDigit;

    //value
    private String defaultValue;
    private String isNullable;
    private Object value;

    //
    private String comment;

    //key
    private String keyType;

    @Override
    public TableFieldInfo clone() {
        TableFieldInfo tableFieldInfo = null;
        try{
            tableFieldInfo = (TableFieldInfo)super.clone();
        }catch(CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return tableFieldInfo;
    }
}
