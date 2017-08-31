package com.wjj.datax.es.common;

import com.alibaba.datax.common.spi.ErrorCode;
import org.omg.CORBA.UNKNOWN;

/**
 * @author wangjiajun
 * @date 2017/8/28 9:39
 */
public enum ElasticsearchErrorCode implements ErrorCode{
    COLUMN_COUNT_ERROR("columnCountError","配置文件column数量大于reader column数量"),
    INDEX_OR_TYPE_EMPTY_ERROR("indexOrTypeEmptyError","索引和类型不能为空"),
    UNKNOWN_DATA_TYPE("unknownDataType","未知的数据类型");

    private final String code;
    private final String msg;
    private ElasticsearchErrorCode(String code,String msg){
        this.code = code;
        this.msg = msg;
    }
    @Override
    public String getCode() {
        return this.code;
    }
    @Override
    public String getDescription() {
        return this.msg;
    }
    @Override
    public String toString() {
        return String.format("Code:[%s], Describe:[%s]. ", this.code,
                this.msg);
    }
}
