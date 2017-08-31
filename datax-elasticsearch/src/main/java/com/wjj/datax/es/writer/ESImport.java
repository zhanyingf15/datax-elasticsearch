package com.wjj.datax.es.writer;
/**
 * @Author wangjiajun
 * @Date 2017/8/21 17:54
 */

import com.alibaba.datax.common.exception.DataXException;
import com.wjj.datax.es.common.ElasticsearchErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 数据批量导入工具，在数据达到指定条件时，批量导入到elasticsearch<br/>
 * 触发条件：达到指定的bulkNum(默认5000)或等待时间达到指定时间(默认10s)或数据大小达到指定大小(默认5M)<br/>
 * 数据导入完毕后需要调用close()方法关闭批处理器。<br/>
 * 导入逻辑：<br/>
 * 1、不指定idFieldName，使用自增长id执行插入操作<br/>
 * 2、指定idFieldName，如果存在记录执行更新操作，如果不存在记录，用指定id值(不能为空)作为文档id执行插入操作
 *
 */
@SuppressWarnings("all")
public class ESImport {
    private static Logger logger = LoggerFactory.getLogger(ESImport.class);
    private BulkProcessor bulkProcessor;
    private String index;
    private String type;
    private String idFieldName; //导入数据中对应ES中_id的字段名
    private Client client = null;
    private int bulkNum = 5000;
    private List<String> failureList = new LinkedList<>();

    private int flushInterval = 10;
    private int bulkSize = 5;
    private int concurrent = 1;
    private boolean refresh = false;

    /**
     * 使用自增长id作为es数据的id
     */
    public ESImport(Client client,String index, String type){
        this.client = client;
        this.index = index;
        this.type = type;
    }
    /**
     * 使用自增长id作为es数据的id
     * @param bulkSize 每个批次的最大处理数量
     */
    public ESImport(Client client,String index, String type, int bulkNum){
        this.client = client;
        this.index = index;
        this.type = type;
        this.bulkNum = bulkNum;
    }
    /**
     * @param idFieldName 使用指定的id字段值作为es数据的id
     */
    public ESImport(Client client,String index, String type, String idFieldName){
        this.client = client;
        this.index = index;
        this.type = type;
        this.idFieldName = idFieldName;
    }
    /**
     * @param idFieldName 使用指定的id字段值作为es数据的id
     * @param bulkSize 每个批次的最大处理数量
     */
    public ESImport(Client client,String index, String type, String idFieldName, int bulkNum){
        this.client = client;
        this.index = index;
        this.type = type;
        this.idFieldName = idFieldName;
        this.bulkNum = bulkNum;
    }

    /**
     * 构建批处理器
     * @return
     */
    public ESImport build(){
        if(StringUtils.isBlank(index)|| StringUtils.isBlank(type)){
            throw DataXException.asDataXException(ElasticsearchErrorCode.INDEX_OR_TYPE_EMPTY_ERROR,"");
        }
        BulkProcessor.Builder bulkBuilder =  BulkProcessor.builder(client,new BulkProcessorListenr());
        bulkBuilder.setBulkActions(bulkNum)
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
                .setConcurrentRequests(this.concurrent);
        bulkProcessor = bulkBuilder.build();
        return this;
    }
    /**
     * 设置批处理容量阀值，到达该值时触发导入操作，设置-1禁用
     */
    public ESImport setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
        return this;
    }
    public ESImport setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }
    public ESImport setBulkNum(int bulkNum) {
        this.bulkNum = bulkNum;
        return this;
    }
    public ESImport setConcurrent(int concurrent){
        this.concurrent = concurrent;
        return this;
    }
    public ESImport setRefresh(boolean refresh){
        this.refresh = refresh;
        return this;
    }
    /**
     * 关闭批处理器,如果有文档未完成，则等待完成，最多等待awaitMinutes分钟
     */
    public void awaitClose(int awaitMinutes){
        try {
            if(awaitMinutes<=0){
                bulkProcessor.close();
            }else{
                bulkProcessor.awaitClose(awaitMinutes, TimeUnit.MINUTES);
            }
        } catch (InterruptedException e) {
            logger.error("关闭bulkProcessor出错",e);
        }
    }

    /**
     * 获取失败列表
     */
    public List<String> getFailureList(){
        return this.failureList;
    }
    /**
     * 将数据导入到ES
     * @param dataList  数据列表
     * @return
     */
    public void add(List<Map<String, Object>> dataList) {
        for(Map<String, Object> data:dataList){
            add(data);
        }
    }
    /**
     * 将数据导入到ES
     * @param data  数据
     * @return
     */
    public void add(Map<String, Object> data) {
        if(bulkProcessor==null){
            throw new RuntimeException("未构建批处理器，请调用build()方法构建");
        }
        if(StringUtils.isBlank(idFieldName)){//不指定id字段，使用自增长id执行插入操作
            IndexRequestBuilder builder = client.prepareIndex(index,type).setSource(data);
            bulkProcessor.add(builder.request());
        }else{//执行更新或插入操作
            String esId = (String) data.get(idFieldName);
            if(StringUtils.isBlank(esId)){
                failureList.add(esId);
            }else{
                if(StringUtils.equals("_id",idFieldName)){
                    data.remove("_id");
                }
                UpdateRequestBuilder builder = client.prepareUpdate(index,type,esId)
                        .setDocAsUpsert(true)
                        .setDoc(data);
                bulkProcessor.add(builder.request());
            }
        }
    }
    private class BulkProcessorListenr implements BulkProcessor.Listener{
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            request.refresh(refresh);
            int importSize = request.numberOfActions();
            logger.debug("批次号：{}，开始导入{}条数据",executionId,importSize);
        }
        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            int importSize = request.numberOfActions();
            boolean hasFailure = response.hasFailures();
            int failureCount = 0;
            for(BulkItemResponse res:response.getItems()){
                if(res.isFailed()){
                    failureCount++;
                    String id = res.getId();
                    failureList.add(id);
                    logger.error("导入失败，id:"+id,res.getFailure().getCause());
                }
            }
            logger.debug("批次号：{}，导入{}条数据成功，{}条数据失败，耗时：{}ms",executionId,(importSize-failureCount),failureCount,response.getTookInMillis());
        }
        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            logger.error("批次号："+executionId+"，发生异常，导入数据失败",failure);
        }
    }
}
