package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

//从集合中读取数据
public class Demo01 {
    public static void main(String[] args) throws Exception {
        List<WaterSensor> waterSensors = Arrays.asList(new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromCollection(waterSensors).print();
        executionEnvironment.execute();

    }
}
