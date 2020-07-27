package com.alibaba.datax.plugin.reader.httpreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson.JSONObject;

import java.util.Date;
import java.util.Objects;

/**
 * 字段类型匹配解析
 */
public enum ColumnTypeEnum implements ColumnValueI{

    /***/
    STRING{
        @Override
        public Column getColumnValue(JSONObject item, Object columnName){
            final Object datum = item.get(columnName.toString());
            if(Objects.isNull(datum) || datum instanceof String){
                return new StringColumn((String)datum);
            }
            throw DataXException.asDataXException(HttpReaderErrorCode.COLUMN_TYPE_ERROR,
                String.format("[%s]类型配置错误", columnName));
        }
    },
    DOUBLE{
        @Override
        public Column getColumnValue(JSONObject item, Object columnName){
            final Object datum = item.get(columnName.toString());
            if(Objects.isNull(datum) || datum instanceof Double){
                return new DoubleColumn((Double)datum);
            }
            throw DataXException.asDataXException(HttpReaderErrorCode.COLUMN_TYPE_ERROR,
                String.format("[%s]类型配置错误", columnName));
        }
    },
    LONG{
        @Override
        public Column getColumnValue(JSONObject item, Object columnName){
            final Object datum = item.get(columnName.toString());
            if(Objects.isNull(datum) || datum instanceof Long){
                return new LongColumn((Long)datum);
            }
            throw DataXException.asDataXException(HttpReaderErrorCode.COLUMN_TYPE_ERROR,
                String.format("[%s]类型配置错误", columnName));
        }
    },
    DATE{
        @Override
        public Column getColumnValue(JSONObject item, Object columnName){
            final Object datum = item.get(columnName.toString());
            if(Objects.isNull(datum) || datum instanceof Date){
                return new DateColumn((Date)datum);
            }
            throw DataXException.asDataXException(HttpReaderErrorCode.COLUMN_TYPE_ERROR,
                String.format("[%s]类型配置错误", columnName));
        }
    },
    BOOLEAN{
        @Override
        public Column getColumnValue(JSONObject item, Object columnName){
            final Object datum = item.get(columnName.toString());
            if(Objects.isNull(datum) || datum instanceof Boolean){
                return new BoolColumn((Boolean)datum);
            }
            throw DataXException.asDataXException(HttpReaderErrorCode.COLUMN_TYPE_ERROR,
                String.format("[%s]类型配置错误", columnName));
        }
    },
    INT{
        @Override
        public Column getColumnValue(JSONObject item, Object columnName){
            final Object datum = item.get(columnName.toString());
            if(Objects.isNull(datum) || datum instanceof Integer){
                return new LongColumn((Integer)datum);
            }
            throw DataXException.asDataXException(HttpReaderErrorCode.COLUMN_TYPE_ERROR,
                String.format("[%s]类型配置错误", columnName));
        }
    },

}
interface ColumnValueI {
    public Column getColumnValue(JSONObject item, Object columnName);
}

