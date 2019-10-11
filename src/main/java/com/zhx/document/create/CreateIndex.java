package com.zhx.document.create;

import com.zhx.common.ElasticsearchCloseException;
import com.zhx.common.ElasticsearchConnection;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;

/**
 * 创建索引
 * 参考文档：
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.3/java-rest-high-document-index.html#_providing_the_document_source
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.3/java-rest-high-document-bulk.html
 * TODO: 异步创建索引
 */
public class CreateIndex extends ElasticsearchConnection{

    /**
     * 同步创建单个索引
     * @throws IOException
     * @throws ElasticsearchCloseException
     */
    public void sync() throws IOException, ElasticsearchCloseException {

        //创建一个索引请求对象
        IndexRequest indexRequest = new IndexRequest("people")
                .opType(DocWriteRequest.OpType.INDEX)//INDEX不用指定id，CREATE必须指定id
                .source("user", "zhx",
                        "age", 24,
                        "job", "programmer")
                .timeout("1s");//从发送请求到主分片可用的超时时间

        //同步创建
        IndexResponse indexResponse = getConnection().index(indexRequest, RequestOptions.DEFAULT);

        System.out.println(indexResponse);
    }

    /**
     * 同步批量创建索引
     * @throws IOException
     * @throws ElasticsearchCloseException
     */
    public void syncBulk() throws IOException, ElasticsearchCloseException {

        IndexRequest ymsRequest = new IndexRequest("people")
                .opType(DocWriteRequest.OpType.INDEX)
                .source("user", "yms",
                        "age", 25,
                        "job", "programmer")
                .timeout("1s");
        IndexRequest jyqRequest = new IndexRequest("people")
                .opType(DocWriteRequest.OpType.INDEX)
                .source("user", "jyq",
                        "age", 26,
                        "job", "programmer")
                .timeout("1s");
        IndexRequest ccmRequest = new IndexRequest("people")
                .opType(DocWriteRequest.OpType.INDEX)
                .source("user", "ccm",
                        "age", 24,
                        "job", "hrbp")
                .timeout("1s");

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(ymsRequest);
        bulkRequest.add(jyqRequest);
        bulkRequest.add(ccmRequest);

        BulkResponse response = getConnection().bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response);
    }



    public static void main(String[] args) {
        try {
            CreateIndex createIndex = new CreateIndex();
//            createIndex.sync();
            createIndex.syncBulk();
            createIndex.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ElasticsearchCloseException e) {
            e.printStackTrace();
        }


    }



}
