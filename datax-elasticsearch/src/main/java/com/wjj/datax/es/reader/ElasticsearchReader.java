package com.wjj.datax.es.reader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.wjj.datax.es.common.ConfigConstants;
import com.wjj.datax.es.common.ESClient;
import com.wjj.datax.es.common.ElasticsearchErrorCode;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author wangjiajun
 * @date 2017/8/28 11:05
 */
@SuppressWarnings("all")
public class ElasticsearchReader extends Reader {

    public static class Job extends Reader.Job{
        private static Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration configuration = null;
        private Client client;
        private String index = "";
        private String type = "";
        private int shardsNumber = 1;
        @Override
        public void init() {
            logger.info("=================elasticsearch reader job init======================");
            configuration = this.getPluginJobConf();
            this.index = configuration.getString(ConfigConstants.INDEX);
            this.type = configuration.getString(ConfigConstants.TYPE);
            List connection = configuration.getList(ConfigConstants.CONNECTION);
            client = ESClient.getNewClient(connection);
            GetSettingsResponse response = client.admin().indices().prepareGetSettings(this.index).get();
            for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
                String index = cursor.key;
                Settings settings = cursor.value;
                Integer shards = settings.getAsInt("index.number_of_shards", null);
                if(StringUtils.equals(index,this.index)){
                    this.shardsNumber = shards;
                }
            }
            this.client.close();
        }
        @Override
        public List<Configuration> split(int adviceNumber) {
            int taskNum = adviceNumber;
            if(this.shardsNumber<=taskNum){
                taskNum = this.shardsNumber;
            }
            int shardsCode[] = new int[this.shardsNumber];
            for(int i=0;i<this.shardsNumber;i++){
                shardsCode[i] = i;
            }
            int shardNumPerTask = (int)Math.round(((double)this.shardsNumber)/taskNum);
            List<Configuration> configs = new ArrayList<>();
            for(int i=0;i<taskNum;i++){
                int start = i*shardNumPerTask;
                int end = start+shardNumPerTask;
                if(end>shardsCode.length||(i==taskNum-1&&end<shardsCode.length)){
                    end = shardsCode.length;
                }
                int[] shardsArr = ArrayUtils.subarray(shardsCode,start,end);
                Configuration shardConfig = this.configuration.clone();
                shardConfig.set("shards",StringUtils.join(shardsArr,",".charAt(0)));
                configs.add(shardConfig);
            }
            return configs;
        }
        @Override
        public void destroy() {
            logger.info("============elasticsearch reader job destroy=================");
        }
    }

    public static class Task extends Reader.Task{
        private static Logger logger = LoggerFactory.getLogger(Task.class);
        private Client client;
        private Configuration configuration = null;
        private String index = "";
        private String type = "";
        private List<String> column = null;
        private String scrollId;
        private TimeValue keepAlive = TimeValue.timeValueMinutes(3);
        private int pageSize = 100;
        private String shards;
        @Override
        public void init() {
            logger.info("=================elasticsearch reader task init======================");
            configuration = super.getPluginJobConf();
            this.index = configuration.getString(ConfigConstants.INDEX);
            this.type = configuration.getString(ConfigConstants.TYPE);
            this.column = configuration.getList(ConfigConstants.COLUMN,String.class);
            this.pageSize = configuration.getInt(ConfigConstants.PAGE_SIZE,this.pageSize);
            List connection = configuration.getList(ConfigConstants.CONNECTION);
            this.shards = configuration.getString("shards");
            this.client = ESClient.getNewClient(connection);
        }
        @Override
        public void startRead(RecordSender recordSender) {
            logger.info("=============elasticsearch reader task start read on shards:"+this.shards+"==================");
            SearchResponse response = null;
            while(true){
                if(StringUtils.isBlank(this.scrollId)){
                    SearchRequestBuilder builder = this.client.prepareSearch(this.index)
                            .setTypes(this.type)
                            .setScroll(this.keepAlive)
                            .setSearchType(SearchType.DFS_QUERY_AND_FETCH)
                            .setPreference(Preference.SHARDS.type()+":"+this.shards)
                            .setSize(this.pageSize);
                    response = builder.execute().actionGet();
                }else{
                    response = this.client.prepareSearchScroll(this.scrollId)
                            .setScroll(this.keepAlive)
                            .execute()
                            .actionGet();
                    if(response.getHits().getHits().length == 0){
                        logger.info("=================elasticsearch reader task end read======================");
                        recordSender.flush();
                        recordSender.terminate();
                        return;
                    }
                }
                this.scrollId = response.getScrollId();
                SearchHit[] hits = response.getHits().getHits();
                for(int i=0;i<hits.length;i++){
                    SearchHit hit = hits[i];

                    Map<String,Object> data = hit.getSource();
                    Record record =  recordSender.createRecord();

                    String _id = hit.getId();
                    Column _idCol = new StringColumn(_id);//es的doc id
                    if(this.column.size()==1&&StringUtils.equals("*",this.column.get(0))){
                        record.addColumn(_idCol);
                        Iterator<Map.Entry<String,Object>> iterator = data.entrySet().iterator();
                        while(iterator.hasNext()){
                            Map.Entry<String,Object> entry = iterator.next();
                            Column col = getColumn(_id,entry.getKey(),entry.getValue());
                            record.addColumn(col);
                        }
                    }else{
                        for(int j=0;j<this.column.size();j++){
                            String key = this.column.get(j);
                            if(StringUtils.equals("_id",key)){
                                record.addColumn(_idCol);
                            }else{
                                Column col = getColumn(_id,key,data.get(key));
                                record.addColumn(col);
                            }

                        }
                    }
                    recordSender.sendToWriter(record);
                }
            }
        }
        @Override
        public void destroy() {
            logger.info("======elasticsearch reader task destroy==============");
            if(StringUtils.isNotBlank(this.scrollId)){
                this.client.prepareClearScroll().addScrollId(this.scrollId).execute().actionGet();
            }
            this.client.close();
        }
        private Column getColumn(String _id,String key,Object value){
            if(value==null){
                return null;
            }
            Column col = null;
            if(value instanceof Long){
                col = new LongColumn((Long)value);
            }else if(value instanceof Integer){
                col = new LongColumn(((Integer) value).longValue());
            }else if(value instanceof Byte){
                col = new LongColumn(((Byte) value).longValue());
            }else if(value instanceof Short){
                col = new LongColumn(((Short) value).longValue());
            }else if(value instanceof String){
                col = new StringColumn((String) value);
            }else if(value instanceof Double){
                col = new DoubleColumn((Double) value);
            }else if(value instanceof Float){
                col = new DoubleColumn(((Float) value).doubleValue());
            }else if(value instanceof Date){
                col = new DateColumn((Date) value);
            }else if(value instanceof Boolean){
                col = new BoolColumn((Boolean) value);
            }else if(value instanceof byte[]){
                col = new BytesColumn((byte[]) value);
            }else{
                throw DataXException.asDataXException(ElasticsearchErrorCode.UNKNOWN_DATA_TYPE,"发生在_id:"+_id+",key:"+key);
            }
            return col;
        }
    }
}
