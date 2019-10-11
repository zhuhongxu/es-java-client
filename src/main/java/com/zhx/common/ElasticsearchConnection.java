package com.zhx.common;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * Elasticsearch连接
 */
public class ElasticsearchConnection {

    private static RestHighLevelClient client = null;


    static {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1", 9200, "http")
                )
        );
    }

    /**
     * 获取es连接
     * @return
     */
    public RestHighLevelClient getConnection(){
        return this.client;
    }


    /**
     * 关闭es连接
     * @throws IOException
     * @throws ElasticsearchCloseException
     */
    public void close() throws IOException, ElasticsearchCloseException {
        if (null != client){
            client.close();
        } else {
            throw new ElasticsearchCloseException("client为空");
        }
    }

}
