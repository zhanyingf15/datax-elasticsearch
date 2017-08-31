package com.wjj.datax.es.writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.wjj.datax.es.common.ConfigConstants;
import com.wjj.datax.es.common.ESClient;
import com.wjj.datax.es.common.ElasticsearchErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wangjiajun
 * @date 2017/8/25 18:54
 */
@SuppressWarnings("all")
public class ElasticsearchWriter extends Writer {
    public static class Job extends Writer.Job{
        private static Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration configuration = null;
        @Override
        public void init() {
            logger.info("=================elasticsearch writer job init======================");
            this.configuration = this.getPluginJobConf();
        }
        public  List<Configuration> split(int adviceNumber){
            List<Configuration> configs = new ArrayList<>();
            for(int i=0;i<adviceNumber;i++){
                this.configuration.set("concurrent",adviceNumber);
                configs.add(this.configuration);
            }
            return configs;
        }
        @Override
        public void destroy() {
            Task.esImport.awaitClose(5);
            logger.info("============elasticsearch writer job destroy=================");
        }
    }
    public static class Task extends Writer.Task{
        private static Logger logger = LoggerFactory.getLogger(Task.class);
        public static volatile ESImport esImport;
        private Configuration configuration = null;
        public String index = "";
        public String type = "";
        public String idField = "";
        public List<String> column = null;
        public List connection = null;
        public int concurrent = 1;
        public int bulkNum = 1000;
        public boolean refresh = false;
        @Override
        public void init() {
            logger.info("=================elasticsearch writer task init======================");
            this.configuration = super.getPluginJobConf();
            this.index = this.configuration.getString(ConfigConstants.INDEX);
            this.type = this.configuration.getString(ConfigConstants.TYPE);
            this.idField = this.configuration.getString(ConfigConstants.ID_FIELD,"").trim();
            this.column = this.configuration.getList(ConfigConstants.COLUMN,String.class);
            this.connection = this.configuration.getList(ConfigConstants.CONNECTION);
            this.concurrent = this.configuration.getInt("concurrent",this.concurrent);
            this.bulkNum = this.configuration.getInt(ConfigConstants.BULK_Num,this.bulkNum);
            this.refresh = this.configuration.getBool(ConfigConstants.REFRESH,false);
            initESImport(this);
        }
        private synchronized static void initESImport(Task task){
            if(Task.esImport==null){
                Task.esImport = new ESImport(ESClient.getNewClient(task.connection),task.index,task.type,task.idField)
                        .setBulkNum(task.bulkNum)
                        .setConcurrent(task.concurrent)
                        .setRefresh(task.refresh)
                        .build();
            }
        }
        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            logger.info("=================elasticsearch writer task start write======================");
            Record record = null;
            while (true){
                record = recordReceiver.getFromReader();
                if(record==null){
                    logger.info("=================elasticsearch writer task end write======================");
                    recordReceiver.shutdown();
                    return;
                }
                if(this.column.size()>record.getColumnNumber()){
                    throw DataXException.asDataXException(ElasticsearchErrorCode.COLUMN_COUNT_ERROR,"");
                }

                Map<String,Object> data = new HashMap<>();
                for(int i=0;i<this.column.size();i++){
                    Column readColumn = record.getColumn(i);
                    if(readColumn!=null){
                        data.put(this.column.get(i),readColumn.getRawData());
                    }
                }
                Task.esImport.add(data);
            }
        }
        @Override
        public void destroy() {
            logger.info("======elasticsearch writer task destroy==============");
        }
    }
}
