package com.atguigu.flink.chapter05;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.*;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.*;

//Sink ElasticSearch
public class Demo26 {
    public static void main(String[] args) throws Exception {

        List<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        List<HttpHost> httpHosts = Arrays.asList(new HttpHost("hadoop102", 9200), new HttpHost("hadoop103", 9200), new HttpHost("hadoop104", 9200));

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromCollection(waterSensors).addSink(new ElasticsearchSink.Builder<>(httpHosts, (WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) ->{
                // 1. 创建es写入请求
                IndexRequest request = Requests
                        .indexRequest("sensor")
                        .type("_doc")
                        .id(waterSensor.getId())
                        .source(JSON.toJSONString(waterSensor), XContentType.JSON);
                // 2. 写入到es
                requestIndexer.add(request);
        }).build());

        executionEnvironment.execute();
    }
}
