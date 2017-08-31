package com.wjj.datax.es.common;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * @author wangjiajun
 * @date 2017/8/25 19:23
 */
public class ESClient {
    private static Client client;
    private static Logger logger = LoggerFactory.getLogger(ESClient.class);
    private static Client createClient(List<String> urls){
        TransportClient transportClient = null;
        Settings settings = Settings.builder().put("client.transport.ignore_cluster_name", true).build();
        try {
            transportClient = TransportClient.builder().settings(settings).build();

            for (String hostAndPort : urls) {
                String host = hostAndPort.split(":")[0];
                String port = hostAndPort.split(":")[1];
                transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), Integer.parseInt(port)));
            }
        } catch (UnknownHostException e) {
            logger.error("",e);
        }
        return transportClient;
    }
    public static Client getNewClient(List<String> urls){
        return createClient(urls);
    }
    public static Client getSingleClient(List<String> urls){
        if(client==null){
            synchronized (ESClient.class){
                client = createClient(urls);
                Client temp = client;
                if(temp==null){
                    synchronized (ESClient.class){
                        temp = createClient(urls);
                    }
                    client = temp;
                }
            }
        }
        return client;
    }

}
