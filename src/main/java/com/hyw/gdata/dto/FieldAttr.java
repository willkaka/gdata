package com.hyw.gdata.dto;

import com.hyw.gdata.utils.QueryUtil;
import lombok.Data;
import lombok.experimental.Accessors;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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

    public FieldAttr(){ }
    public FieldAttr(ResultSet set) throws SQLException {
        ResultSetMetaData metaData = set.getMetaData();
        for (int fieldNum = 1; fieldNum <= metaData.getColumnCount(); fieldNum++) {
            if (metaData.getColumnName(fieldNum) != null && !"".equals(metaData.getColumnName(fieldNum))) {
                String fieldName;
                if (QueryUtil.isNotBlankStr(metaData.getColumnLabel(fieldNum))) {
                    fieldName = metaData.getColumnLabel(fieldNum);
                } else {
                    fieldName = metaData.getColumnName(fieldNum);
                }
                Object fieldValue = set.getObject(fieldName);
                switch (fieldName) {
                    case "SCOPE_TABLE": {this.setScopeTable(String.valueOf(fieldValue)); break;}
                    case "TABLE_CAT": {this.setTableCat(String.valueOf(fieldValue)); break;}
                    case "BUFFER_LENGTH": {this.setBufferLength(null==fieldValue?0:(int)fieldValue); break;}
                    case "IS_NULLABLE": {this.setIsNullable(String.valueOf(fieldValue)); break;}
                    case "TABLE_NAME": {this.setTableName(String.valueOf(fieldValue)); break;}
                    case "COLUMN_DEF": {this.setColumnDef(String.valueOf(fieldValue)); break;}
                    case "SCOPE_CATALOG": {this.setScopeCatalog(String.valueOf(fieldValue)); break;}
                    case "TABLE_SCHEM": {this.setTableSchem(String.valueOf(fieldValue)); break;}
                    case "COLUMN_NAME": {this.setColumnName(String.valueOf(fieldValue)); break;}
                    case "NULLABLE": {this.setNullable(null==fieldValue?0:(int)fieldValue); break;}
                    case "REMARKS": {this.setRemarks(String.valueOf(fieldValue)); break;}
                    case "DECIMAL_DIGITS": {this.setDecimalDigits(String.valueOf(fieldValue)); break;}
                    case "NUM_PREC_RADIX": {this.setNumPrecRadix(null==fieldValue?0:(int)fieldValue); break;}
                    case "SQL_DATETIME_SUB": {this.setSqlDatetimeSub(null==fieldValue?0:(int)fieldValue); break;}
                    case "IS_GENERATEDCOLUMN": {this.setIsGeneratedcolumn(String.valueOf(fieldValue)); break;}
                    case "IS_AUTOINCREMENT": {this.setIsAutoincrement(String.valueOf(fieldValue)); break;}
                    case "SQL_DATA_TYPE": {this.setSqlDataType(null==fieldValue?0:(int)fieldValue); break;}
                    case "CHAR_OCTET_LENGTH": {this.setCharOctetLength(null==fieldValue?0:(int)fieldValue); break;}
                    case "ORDINAL_POSITION": {this.setOrdinalPosition(null==fieldValue?0:(int)fieldValue); break;}
                    case "SCOPE_SCHEMA": {this.setScopeSchema(String.valueOf(fieldValue)); break;}
                    case "SOURCE_DATA_TYPE": {this.setSourceDataType(String.valueOf(fieldValue)); break;}
                    case "DATA_TYPE": {this.setDataType(String.valueOf(fieldValue)); break;}
                    case "TYPE_NAME": {this.setTypeName(String.valueOf(fieldValue)); break;}
                    case "COLUMN_SIZE": {this.setColumnSize(null==fieldValue?0:(int)fieldValue); break;}
                }
            }
        }
    }
}
