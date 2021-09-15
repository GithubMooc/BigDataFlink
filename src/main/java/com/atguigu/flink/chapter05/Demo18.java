package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

//maxBy
public class Demo18 {
    public static void main(String[] args) throws Exception {
        List<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = executionEnvironment.fromCollection(waterSensors).keyBy(WaterSensor::getId);
        waterSensorStringKeyedStream.maxBy("vc",false).print("maxBy");
        executionEnvironment.execute();
    }
}
