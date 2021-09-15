package com.atguigu.flink.chapter05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

// process 从流中获取更多的信息 不仅仅数据本身
public class Demo21 {
    public static void main(String[] args) throws Exception {
        
        List<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromCollection(waterSensors).process(new ProcessFunction<WaterSensor, Tuple2<String,Integer>>() {
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2(waterSensor.getId(),waterSensor.getVc()));
            }
        }).print();
        executionEnvironment.execute();
    }
}
